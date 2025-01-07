import {HttpClient, HttpBodyTimeoutError} from '@subsquid/http-client'
import {AsyncQueue, last, Throttler, wait, withErrorContext} from '@subsquid/util-internal'
import {EvmQuery, EvmResponse} from './query/evm'

export type PortalQuery = EvmQuery

export type PortalResponse<Q extends PortalQuery> = Q extends EvmQuery ? EvmResponse<Q> : any

export type PortalClientOptions = {
    url: string
    http?: HttpClient

    bufferSizeThreshold?: number
    newBlockThreshold?: number
    durationThreshold?: number

    headPollInterval?: number
}

export type PortalRequestOptions = {
    headers?: HeadersInit
    retryAttempts?: number
    retrySchedule?: number[]
    httpTimeout?: number
    bodyTimeout?: number
    abort?: AbortSignal
}

export type PortalStreamOptions = {
    request?: Omit<PortalRequestOptions, 'abort'>

    bufferSizeThreshold?: number
    newBlockThreshold?: number
    durationThreshold?: number

    headPollInterval?: number

    stopOnHead?: boolean
}

export type PortalStreamData<R extends PortalResponse<any>> = {
    finalizedHead: {
        number: number
        hash: string
    }
    blocks: R[]
}

export class PortalClient {
    private url: URL
    private client: HttpClient
    private headPollInterval: number
    private bufferThreshold: number
    private newBlockThreshold: number
    private durationThreshold: number

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.client = options.http || new HttpClient()
        this.headPollInterval = options.headPollInterval ?? 5_000
        this.bufferThreshold = options.bufferSizeThreshold ?? 10 * 1024 * 1024
        this.newBlockThreshold = options.newBlockThreshold ?? 300
        this.durationThreshold = options.durationThreshold ?? 5_000
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
        return height
    }

    getFinalizedQuery<Q extends PortalQuery, R extends PortalResponse<Q>>(
        query: Q,
        options?: PortalRequestOptions
    ): Promise<R[]> {
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

    getFinalizedStream<Q extends PortalQuery, R extends PortalResponse<Q> = PortalResponse<Q>>(
        query: Q,
        options?: PortalStreamOptions
    ): ReadableStream<PortalStreamData<R>> {
        let {headPollInterval, newBlockThreshold, durationThreshold, bufferSizeThreshold, request, stopOnHead} = {
            bufferSizeThreshold: this.bufferThreshold,
            newBlockThreshold: this.newBlockThreshold,
            durationThreshold: this.durationThreshold,
            headPollInterval: this.headPollInterval,
            ...options,
        }

        let abortStream = new AbortController()
        let abortSignal = abortStream.signal
        let buffer = new BlocksBuffer<R>(bufferSizeThreshold)
        let top = new Throttler(async () => {
            let number = await this.getFinalizedHeight()
            return {
                number,
                hash: '0x',
            }
        }, 10_000)

        const ingest = async () => {
            let startBlock = query.fromBlock ?? 0
            let endBlock = query.toBlock ?? Infinity

            let heartbeat: HeartBeat | undefined
            let timeout: ReturnType<typeof setTimeout> | undefined
            let reader: ReadableStreamDefaultReader<string[]> | undefined

            const abort = async () => reader?.cancel()

            while (startBlock <= endBlock && !abortSignal.aborted) {
                try {
                    let archiveQuery = {
                        ...query,
                        fromBlock: startBlock,
                    }
                    let res = await this.client
                        .request<ReadableStream<Buffer>>('POST', this.getDatasetUrl('finalized-stream'), {
                            ...request,
                            json: archiveQuery,
                            stream: true,
                            abort: abortSignal,
                        })
                        .catch(
                            withErrorContext({
                                query: archiveQuery,
                            })
                        )

                    if (res.status === 204) {
                        if (stopOnHead) break
                        await wait(headPollInterval, abortSignal)
                        continue
                    }

                    reader = res.body
                        .pipeThrough(new TextDecoderStream('utf8'))
                        .pipeThrough(new LineSplitStream('\n'))
                        .getReader()
                    abortSignal.addEventListener('abort', abort, {once: true})

                    timeout = setTimeout(() => buffer.ready(), durationThreshold)

                    let heartbeatInterval = Math.ceil(newBlockThreshold / 4)
                    heartbeat = new HeartBeat((diff) => {
                        if (diff > newBlockThreshold) {
                            buffer.ready()
                        }
                    }, heartbeatInterval)

                    while (true) {
                        let lines = await reader?.read()
                        if (lines.done) break

                        heartbeat.pulse()

                        let size = 0
                        let blocks: R[] = []

                        for (let line of lines.value) {
                            let block = JSON.parse(line) as R
                            size += line.length
                            blocks.push(block)
                        }

                        await buffer.put(blocks, size)

                        let lastBlock = last(blocks).header.number
                        startBlock = lastBlock + 1
                    }
                } catch (err) {
                    if (abortSignal.aborted) {
                        // ignore
                    } else if (err instanceof HttpBodyTimeoutError) {
                        // ignore
                    } else {
                        throw err
                    }
                } finally {
                    buffer.ready()

                    heartbeat?.stop()
                    clearTimeout(timeout)
                    abortSignal.removeEventListener('abort', abort)

                    await reader?.cancel().catch(() => {})
                }
            }
        }

        return new ReadableStream<PortalStreamData<R>>({
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

class BlocksBuffer<B> {
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
