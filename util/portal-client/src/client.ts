import {HttpClient} from '@subsquid/http-client'
import type {Logger} from '@subsquid/logger'
import {AsyncQueue, last, Throttler, wait, withErrorContext} from '@subsquid/util-internal'
import assert from 'assert'

export interface HashAndHeight {
    hash: string
    height: number
}

export interface PortalQuery {
    fromBlock: number
    toBlock?: number
}

export interface Block {
    header: {
        hash: string
        number: number
    }
}

export interface PortalClientOptions {
    url: string
    http?: HttpClient
    bufferSizeThreshold?: number
    newBlockTimeout?: number
    headPollInterval?: number
    log?: Logger
}

export interface PortalRequestOptions {
    headers?: HeadersInit
    retryAttempts?: number
    retrySchedule?: number[]
    httpTimeout?: number
    abort?: AbortSignal
}

export interface PortalStreamOptions extends PortalRequestOptions {
    newBlockTimeout?: number
    headPollInterval?: number
}

export interface PortalStreamData<B extends Block> {
    finalizedHead: HashAndHeight
    blocks: B[]
}

export class PortalClient {
    private url: URL
    private client: HttpClient
    private bufferThreshold: number
    private newBlockTimeout: number
    private log?: Logger

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.log = options.log
        this.client = options.http || new HttpClient()
        this.bufferThreshold = options.bufferSizeThreshold ?? 10 * 1024 * 1024
        this.newBlockTimeout = options.newBlockTimeout ?? 120_000
    }

    private getDatasetUrl(path: string): string {
        let u = new URL(this.url)
        if (this.url.pathname.endsWith('/')) {
            u.pathname += path
        } else {
            u.pathname += '/' + path
        }
        return u.toString()
    }

    async getFinalizedHeight(options?: PortalRequestOptions): Promise<number> {
        let res: string = await this.client.get(this.getDatasetUrl('finalized-stream/height'), options)
        let height = parseInt(res)
        assert(Number.isSafeInteger(height))
        return height
    }

    getFinalizedQuery<B extends Block = Block, Q extends PortalQuery = PortalQuery>(
        query: Q,
        options?: PortalRequestOptions
    ): Promise<B[]> {
        // FIXME: is it needed or it is better to always use stream?
        return this.client
            .request<Buffer>('POST', this.getDatasetUrl(`finalized-stream`), {
                ...options,
                json: query,
            })
            .catch(
                withErrorContext({
                    archiveQuery: query,
                })
            )
            .then((res) => {
                let blocks = res.body
                    .toString('utf8')
                    .trimEnd()
                    .split('\n')
                    .map((line) => JSON.parse(line))
                return blocks
            })
    }

    getFinalizedStream<B extends Block = Block, Q extends PortalQuery = PortalQuery>(
        query: Q,
        options?: PortalStreamOptions
    ): ReadableStream<PortalStreamData<B>> {
        let buffer = new BlocksBuffer<B>(this.bufferThreshold)
        let finalizedHead = new Throttler(async () => {
            // we try to emulate behavior of stream header, but we are missing hash
            // https://github.com/subsquid/squid-sdk/blob/7e993e21435aa24c27335beab510b8ce5a2d24b0/solana/solana-data-service/src/main.ts#L95
            let height = await this.getFinalizedHeight()
            return {
                height,
                hash: '',
            }
        }, 10_000)
        let abortStream = new AbortController()
        let abortSignal = options?.abort ? anySignal([options.abort, abortStream.signal]) : abortStream.signal

        const ingest = async () => {
            let startBlock = query.fromBlock
            let endBlock = query.toBlock ?? Infinity

            while (startBlock <= endBlock && !abortSignal.aborted) {
                let ac = new AbortController()
                let heartbeat: HeartBeat | undefined

                try {
                    let archiveQuery = {...query, fromBlock: startBlock}
                    let response = await this.client
                        .request<ReadableStream<Buffer>>('POST', this.getDatasetUrl('finalized-stream'), {
                            json: archiveQuery,
                            stream: true,
                            abort: anySignal([abortSignal, ac.signal]),
                            ...options,
                        })
                        .catch(
                            withErrorContext({
                                query: archiveQuery,
                            })
                        )

                    if (response.status == 204) {
                        await wait(options?.headPollInterval ?? 1000, abortSignal)
                        continue
                    }

                    let hearbeatInterval = Math.ceil(Math.min(this.newBlockTimeout) / 4)
                    heartbeat = new HeartBeat((diff) => {
                        if (diff > this.newBlockTimeout) abortStream.abort()
                    }, hearbeatInterval)

                    await response.body
                        .pipeThrough(
                            new TransformStream({
                                transform(chunk, controller) {
                                    controller.enqueue(chunk)
                                    heartbeat?.tick()
                                },
                            }),
                            {signal: ac.signal}
                        )
                        .pipeThrough(new TextDecoderStream('utf8'))
                        .pipeThrough(new LineSplitStream('\n'))
                        .pipeTo(
                            new WritableStream({
                                write: async (chunk, controller) => {
                                    let lastBlock = await buffer.put(chunk)
                                    startBlock = lastBlock + 1
                                },
                            })
                        )
                } catch (err) {
                    if (abortSignal.aborted) {
                        // FIXME: should we do anything here?
                    } else if (ac.signal.aborted) {
                        this.log?.warn(`resetting stream due to inactivity for ${this.newBlockTimeout} ms`)
                    } else {
                        throw err
                    }
                } finally {
                    heartbeat?.stop()
                    buffer.ready()
                }
            }
        }

        return new ReadableStream({
            start: async (controller) => {
                ingest()
                    .then(() => {
                        buffer.close()
                    })
                    .catch((error) => {
                        if (buffer.isClosed()) return
                        controller.error(error)
                        buffer.close()
                    })
            },
            pull: async (controller) => {
                let value = await buffer.take()
                if (value) {
                    controller.enqueue({
                        finalizedHead: await finalizedHead.get(),
                        blocks: value,
                    })
                } else {
                    controller.close()
                }
            },
            cancel: () => {
                abortStream.abort()
            },
        })
    }
}

class BlocksBuffer<B extends Block> {
    private blocks: B[] = []
    private queue = new AsyncQueue<B[]>(1)
    private size = 0

    constructor(private bufferSizeThreshold: number) {}

    async put(lines: string[]) {
        for (let line of lines) {
            this.size += line.length
            this.blocks.push(JSON.parse(line))
        }

        let lastBlock = last(this.blocks).header.number

        if (this.size > this.bufferSizeThreshold) {
            this.ready()
            await this.queue.wait()
        }

        return lastBlock
    }

    async take() {
        let value = await this.queue.take()
        this.blocks = []
        this.size = 0
        return value
    }

    ready() {
        if (this.blocks.length == 0) return
        this.queue.forcePut(this.blocks)
    }

    close() {
        return this.queue.close()
    }

    isClosed() {
        return this.queue.isClosed()
    }
}

// AbortSignal.any is available only in Node.js >=20.3.0
function anySignal(signals: AbortSignal[]): AbortSignal {
    if ('any' in AbortSignal) return AbortSignal.any(signals)

    const controller = new AbortController()

    for (const signal of signals) {
        if (signal.aborted) {
            controller.abort(signal.reason)
            break
        }

        signal.addEventListener('abort', () => controller.abort(signal.reason), {
            signal: controller.signal,
        })
    }

    return controller.signal
}

class HeartBeat {
    private interval: ReturnType<typeof setInterval> | undefined
    private timestamp: number

    constructor(fn: (diff: number) => void, ms?: number) {
        this.timestamp = Date.now()
        this.interval = setInterval(() => fn(Date.now() - this.timestamp), ms)
    }

    tick() {
        this.timestamp = Date.now()
    }

    stop() {
        clearInterval(this.interval)
    }
}

class LineSplitStream implements ReadableWritablePair<string[], string> {
    private line = ''
    private transform: TransformStream<string, string[]>

    get readable() {
        return this.transform.readable
    }
    get writable() {
        return this.transform.writable
    }

    constructor(separator: string) {
        this.transform = new TransformStream({
            transform: (chunk, controller) => {
                let lines = chunk.split(separator)
                if (lines.length == 1) {
                    this.line += lines[0]
                } else {
                    let result: string[] = []
                    lines[0] = this.line + lines[0]
                    this.line = lines.pop() || ''
                    result.push(...lines)
                    controller.enqueue(result)
                }
            },
            flush: (controller) => {
                if (this.line) {
                    controller.enqueue([this.line])
                    this.line = ''
                }
            },
        })
    }
}
