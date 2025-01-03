import {HttpClient, HttpBodyTimeoutError} from '@subsquid/http-client'
import {AsyncQueue, unexpectedCase, wait, withErrorContext} from '@subsquid/util-internal'

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

    minBytes?: number
    maxBytes?: number
    maxIdleTime?: number
    maxWaitTime?: number

    headPollInterval?: number
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

    minBytes?: number
    maxBytes?: number
    maxIdleTime?: number
    maxWaitTime?: number

    headPollInterval?: number

    stopOnHead?: boolean
}

export interface PortalStreamData<B extends Block> {
    finalizedHead: HashAndHeight
    blocks: B[]
}

export class PortalClient {
    private url: URL
    private client: HttpClient
    private headPollInterval: number
    private minBytes: number
    private maxBytes: number | undefined
    private maxIdleTime: number
    private maxWaitTime: number

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.client = options.http || new HttpClient()
        this.headPollInterval = options.headPollInterval ?? 5_000
        this.minBytes = options.minBytes ?? 10 * 1024 * 1024
        this.maxBytes = options.maxBytes
        this.maxIdleTime = options.maxIdleTime ?? 300
        this.maxWaitTime = options.maxWaitTime ?? 5_000
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
        const {
            headPollInterval = this.headPollInterval,
            minBytes = this.minBytes,
            maxBytes = Math.max(this.maxBytes ?? 0, minBytes),
            maxIdleTime = this.maxIdleTime,
            maxWaitTime = this.maxWaitTime,
            request,
            stopOnHead,
        } = options ?? {}

        let abortStream = new AbortController()
        let queue = new AsyncQueue<PortalStreamData<B> | Error>(1)
        let bytes = 0
        let lastDataTimestamp = Date.now()
        let idleCheckInterval: ReturnType<typeof setInterval> | undefined
        let waitTimeout: ReturnType<typeof setTimeout> | undefined
        let data: PortalStreamData<B> | undefined

        const ready = () => {
            if (queue.isClosed()) return
            if (queue.peek() != null) return
            if (data == null) return
            if (data.blocks.length == 0) return

            clearInterval(idleCheckInterval)
            clearTimeout(waitTimeout)
            queue.forcePut(data)
        }

        const createIdleCheckInterval = () =>
            setInterval(() => {
                return Date.now() - lastDataTimestamp >= maxIdleTime && ready()
            }, maxIdleTime / 2)
        const createWaitTimeout = () => setTimeout(() => ready(), maxWaitTime)

        const ingest = async () => {


            let abortSignal = abortStream.signal
            let reader: ReadableStreamDefaultReader<string[]> | undefined

            const abort = async () => {
                await reader?.cancel()
                queue.close()
            }

            let fromBlock = query.fromBlock
            let toBlock = query.toBlock ?? Infinity

            try {
                abortSignal.addEventListener('abort', abort)

                while (!abortSignal.aborted) {
                    if (fromBlock > toBlock) break

                    try {
                        let res = await this.getFinalizedStreamRaw(
                            {
                                ...query,
                                fromBlock,
                            },
                            {
                                ...request,
                                abort: abortSignal,
                            }
                        )

                        if (res == null) {
                            if (stopOnHead) break
                            await wait(headPollInterval, abortSignal)
                        } else {
                            idleCheckInterval = createIdleCheckInterval()
                            waitTimeout = createWaitTimeout()

                            reader = res.data.getReader()

                            while (true) {
                                let lines = await reader.read()
                                if (lines.done) break

                                lastDataTimestamp = Date.now()

                                if (data == null) {
                                    data = {finalizedHead: res.finalizedHead, blocks: []}
                                } else {
                                    data.finalizedHead = res.finalizedHead
                                }

                                for (let line of lines.value) {
                                    let block = JSON.parse(line) as B

                                    bytes += line.length
                                    data.blocks.push(block)

                                    fromBlock = block.header.number + 1
                                }

                                if (bytes >= minBytes) {
                                    ready()
                                }

                                if (bytes >= maxBytes) {
                                    await queue.wait()
                                }
                            }
                        }
                    } catch (err) {
                        if (err instanceof HttpBodyTimeoutError) {
                            // ignore
                        } else {
                            throw err
                        }
                    }

                    ready()
                }
            } catch (err) {
                if (abortSignal.aborted) {
                    // ignore
                } else {
                    throw err
                }
            } finally {
                await reader?.cancel().catch(() => {})
                abortSignal.removeEventListener('abort', abort)
                clearInterval(idleCheckInterval)
                clearTimeout(waitTimeout)
            }
        }

        return new ReadableStream<PortalStreamData<B>>({
            start: () => {
                ingest()
                    .then(() => queue.close())
                    .catch((e) => {
                        if (queue.isClosed()) return
                        queue.forcePut(e)
                        queue.close()
                    })
            },
            pull: async (controller) => {
                let value = await queue.take()

                if (value instanceof Error) {
                    controller.error(value)
                } else if (value != null) {
                    controller.enqueue(value)

                    // reset
                    data = undefined
                    bytes = 0
                } else {
                    controller.close()
                }
            },
            cancel: () => {
                abortStream.abort()
            },
        })
    }

    private async getFinalizedStreamRaw(query: PortalQuery, options?: PortalRequestOptions) {
        // NOTE: we emulate the same behaviour as will be implemented for hot blocks stream,
        // but unfortunately we don't have any information about finalized block hash at the moment
        let finalizedHead = {
            height: await this.getFinalizedHeight(options),
            hash: '',
        }

        let res = await this.client
            .request<ReadableStream<Buffer>>('POST', this.getDatasetUrl('finalized-stream'), {
                ...options,
                json: query,
                stream: true,
            })
            .catch(
                withErrorContext({
                    query,
                })
            )

        switch (res.status) {
            case 200:
                return {
                    finalizedHead,
                    data: res.body.pipeThrough(new TextDecoderStream('utf8')).pipeThrough(new LineSplitStream('\n')),
                }
            case 204:
                return undefined
            default:
                throw unexpectedCase(res.status)
        }
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
