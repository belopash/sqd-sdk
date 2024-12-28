import {HttpClient, HttpResponse} from '@subsquid/http-client'
import type {Logger} from '@subsquid/logger'
import {AsyncQueue, last, Throttler, wait, withErrorContext} from '@subsquid/util-internal'
import {addStreamTimeout} from '@subsquid/util-timeout'
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
    newBlockThreshold?: number
    durationThreshold?: number

    headPollInterval?: number
    streamBodyTimeout?: number

    log?: Logger
}

export interface PortalRequestOptions {
    headers?: HeadersInit
    retryAttempts?: number
    retrySchedule?: number[]
    httpTimeout?: number
    bodyTimeout?: number
    abort?: AbortSignal
}

export interface PortalStreamOptions {
    request?: Omit<PortalRequestOptions, 'abort'>

    bufferSizeThreshold?: number
    newBlockThreshold?: number
    durationThreshold?: number

    headPollInterval?: number
    streamBodyTimeout?: number

    stopOnHead?: boolean
}

export interface PortalStreamData<B extends Block> {
    finalizedHead: HashAndHeight
    blocks: B[]
}

export interface PortalStreamData<B extends Block> {
    finalizedHead: HashAndHeight
    blocks: B[]
}

export class PortalClient {
    private url: URL
    private client: HttpClient
    private headPollInterval: number
    private bufferThreshold: number
    private newBlockThreshold: number
    private durationThreshold: number
    private streamBodyTimeout: number
    private log?: Logger

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.log = options.log
        this.client = options.http || new HttpClient()
        this.headPollInterval = options.headPollInterval ?? 5_000
        this.bufferThreshold = options.bufferSizeThreshold ?? 10 * 1024 * 1024
        this.newBlockThreshold = options.newBlockThreshold ?? 300
        this.durationThreshold = options.durationThreshold ?? 5_000
        this.streamBodyTimeout = options.streamBodyTimeout ?? 60_000
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
        let {headPollInterval, newBlockThreshold, durationThreshold, bufferSizeThreshold, streamBodyTimeout, request} =
            {
                bufferSizeThreshold: this.bufferThreshold,
                newBlockThreshold: this.newBlockThreshold,
                durationThreshold: this.durationThreshold,
                headPollInterval: this.headPollInterval,
                streamBodyTimeout: this.streamBodyTimeout,
                ...options,
            }

        let abortStream = new AbortController()
        let buffer = new BlocksBuffer<B>(bufferSizeThreshold)
        let top = new Throttler(async () => {
            let height = await this.getFinalizedHeight()
            return {
                height,
                hash: '0x',
            }
        }, 10_000)

        const ingest = async () => {
            let startBlock = query.fromBlock
            let endBlock = query.toBlock ?? Infinity

            let heartbeat: HeartBeat | undefined
            let timeout: ReturnType<typeof setTimeout> | undefined
            let reader: ReadableStreamDefaultReader<string[]> | undefined

            function abort() {
                return reader?.cancel()
            }

            while (startBlock <= endBlock && !abortStream.signal.aborted) {
                let archiveQuery = {...query, fromBlock: startBlock}
                let res: HttpResponse<ReadableStream<Buffer>> | undefined
                try {
                    res = await this.client
                        .request('POST', this.getDatasetUrl('finalized-stream'), {
                            ...request,
                            json: archiveQuery,
                            stream: true,
                            abort: abortStream.signal,
                        })
                        .catch(
                            withErrorContext({
                                query: archiveQuery,
                            })
                        )

                    if (res.status === 204) {
                        if (options?.stopOnHead) break
                        await wait(headPollInterval, abortStream.signal)
                        continue
                    }

                    abortStream.signal.addEventListener('abort', abort, {once: true})

                    reader = addStreamTimeout(
                        res.body,
                        streamBodyTimeout,
                        () => new StreamBodyTimeoutError(streamBodyTimeout)
                    )
                        .pipeThrough(new TextDecoderStream('utf8'))
                        .pipeThrough(new LineSplitStream('\n'))
                        .getReader()

                    let heartbeatInterval = Math.ceil(newBlockThreshold / 4)
                    heartbeat = new HeartBeat((diff) => {
                        if (diff > newBlockThreshold) {
                            buffer.ready()
                        }
                    }, heartbeatInterval)

                    timeout = setTimeout(() => buffer.ready(), durationThreshold)

                    while (true) {
                        let lines = await reader?.read()
                        if (lines.done) break

                        heartbeat.pulse()

                        let size = 0
                        let blocks = lines.value.map((line) => {
                            let block = JSON.parse(line) as B
                            size += line.length
                            return block
                        })

                        await buffer.put(blocks, size)

                        let lastBlock = last(blocks).header.number
                        startBlock = lastBlock + 1

                        if (options?.stopOnHead) {
                            let finalizedHead = await top.get()
                            if (lastBlock >= finalizedHead.height) {
                                await reader?.cancel()
                                break
                            }
                        }
                    }
                } catch (err) {
                    if (abortStream.signal.aborted) {
                    } else if (err instanceof StreamBodyTimeoutError) {
                        this.log?.warn(`resetting stream: ${err.message}`)
                    } else {
                        throw err
                    }
                } finally {
                    await reader?.cancel().catch(() => {})
                    heartbeat?.stop()
                    buffer.ready()
                    clearTimeout(timeout)
                    abortStream.signal.removeEventListener('abort', abort)
                }
            }
        }

        return new ReadableStream<PortalStreamData<B>>({
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
                        finalizedHead: await top.get(),
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
    private queue: AsyncQueue<B[]>
    private size = 0

    constructor(private bufferSizeThreshold: number) {
        this.queue = new AsyncQueue(bufferSizeThreshold)
    }

    async put(blocks: B[], size: number) {
        this.blocks.push(...blocks)
        this.size += size

        if (this.size > this.bufferSizeThreshold) {
            this.ready()
            await this.queue.wait()
        }
    }

    async take() {
        let value = await this.queue.take()
        this.blocks = []
        this.size = 0
        return value
    }

    ready() {
        if (this.blocks.length == 0) return
        if (this.queue.isClosed()) return
        this.queue.forcePut(this.blocks)
    }

    close() {
        return this.queue.close()
    }

    isClosed() {
        return this.queue.isClosed()
    }
}

class HeartBeat {
    private interval: ReturnType<typeof setInterval> | undefined
    private timestamp: number

    constructor(fn: (diff: number) => void, ms?: number) {
        this.timestamp = Date.now()
        this.interval = setInterval(() => fn(Date.now() - this.timestamp), ms)
    }

    pulse() {
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
                controller.terminate()
            },
        })
    }
}

class StreamBodyTimeoutError extends Error {
    constructor(ms: number) {
        super(`stream body timed out after ${ms} ms`)
    }
}
