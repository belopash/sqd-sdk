import {HttpClient, HttpBodyTimeoutError} from '@subsquid/http-client'
import {createFuture, Future, unexpectedCase, wait, withErrorContext} from '@subsquid/util-internal'

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

export interface PortalStreamDataRaw {
    finalizedHead: HashAndHeight
    blocks: string[]
}

export class PortalClient {
    private url: URL
    private client: HttpClient
    private headPollInterval: number
    private minBytes: number
    private maxBytes: number
    private maxIdleTime: number
    private maxWaitTime: number

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.client = options.http || new HttpClient()
        this.headPollInterval = options.headPollInterval ?? 5_000
        this.minBytes = options.minBytes ?? 40 * 1024 * 1024
        this.maxBytes = options.maxBytes ?? this.minBytes
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
        let {
            headPollInterval = this.headPollInterval,
            minBytes = this.minBytes,
            maxBytes = this.maxBytes,
            maxIdleTime = this.maxIdleTime,
            maxWaitTime = this.maxWaitTime,
            request = {},
            stopOnHead = false,
        } = options ?? {}

        return createRedablePortalStream<B>(
            query,
            {
                headPollInterval,
                minBytes,
                maxBytes,
                maxIdleTime,
                maxWaitTime,
                request,
                stopOnHead,
            },
            async (query, options) => {
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
                            stream: res.body
                                .pipeThrough(new TextDecoderStream('utf8'))
                                .pipeThrough(new LineSplitStream('\n')),
                        }
                    case 204:
                        return undefined
                    default:
                        throw unexpectedCase(res.status)
                }
            }
        )
    }
}

function createRedablePortalStream<B extends Block>(
    query: PortalQuery,
    options: Required<PortalStreamOptions>,
    requestStream: (
        query: PortalQuery,
        options?: PortalRequestOptions
    ) => Promise<{finalizedHead: HashAndHeight; stream: ReadableStream<string[]>} | undefined>
): ReadableStream<PortalStreamData<B>> {
    let {headPollInterval, stopOnHead, maxBytes, minBytes, request, maxIdleTime, maxWaitTime} = options
    maxBytes = Math.max(maxBytes, minBytes)

    let abortStream = new AbortController()

    let buffer: {data: PortalStreamData<B>; bytes: number} | undefined
    let isReady = false
    let isClosed = false

    let pullFuture: Future<void> | undefined
    let putFuture: Future<void> | undefined

    let lastChunkTimestamp = Date.now()
    let idleInterval: ReturnType<typeof setInterval> | undefined

    let lastPullTimestamp = Date.now()
    let putTimeout: ReturnType<typeof setTimeout> | undefined

    async function waitPut() {
        if (isClosed) return
        if (isReady && buffer != null) return

        createPutTimeout()

        putFuture = putFuture || createFuture()
        await putFuture.promise()

        destroyPutTimeout()
    }

    async function waitPull() {
        if (isClosed) return
        pullFuture = pullFuture || createFuture()
        await pullFuture.promise()
    }

    function ready() {
        if (isReady) return
        isReady = true
        destroyIdleInterval()
        destroyPutTimeout()
    }

    function close() {
        if (isClosed) return
        isClosed = true
        putFuture = putFuture?.resolve() ?? undefined
        pullFuture = pullFuture?.resolve() ?? undefined
        destroyIdleInterval()
        destroyPutTimeout()
    }

    function createIdleInterval() {
        clearInterval(idleInterval)

        idleInterval = setInterval(() => {
            if (Date.now() - lastChunkTimestamp >= maxIdleTime) {
                ready()
                if (buffer != null) {
                    putFuture = putFuture?.resolve() ?? undefined
                }
            }
        }, Math.ceil(maxIdleTime / 2))
    }

    function destroyIdleInterval() {
        clearInterval(idleInterval)
        idleInterval = undefined
    }

    function createPutTimeout() {
        clearTimeout(putTimeout)

        let diff = Date.now() - lastPullTimestamp

        putTimeout = setTimeout(() => {
            ready()
            if (buffer != null) {
                putFuture = putFuture?.resolve() ?? undefined
            }
        }, maxWaitTime - diff)
    }

    function destroyPutTimeout() {
        clearTimeout(putTimeout)
        putTimeout = undefined
    }

    async function ingest() {
        let reader: ReadableStreamDefaultReader<string[]> | undefined

        function abort() {
            reader?.cancel().catch(() => {})
            pullFuture?.resolve()
        }

        let abortSignal = abortStream.signal
        abortSignal.addEventListener('abort', abort)

        try {
            let fromBlock = query.fromBlock
            let toBlock = query.toBlock ?? Infinity

            while (true) {
                if (abortSignal.aborted) break
                if (fromBlock > toBlock) break

                let res = await requestStream(
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
                    let {finalizedHead, stream} = res
                    reader = stream.getReader()

                    while (true) {
                        if (idleInterval == null && !isReady) {
                            lastChunkTimestamp = Date.now()
                            createIdleInterval()
                        }

                        let data = await reader.read()
                        if (data.done) break
                        if (data.value.length == 0) continue

                        lastChunkTimestamp = Date.now()

                        if (buffer == null) {
                            buffer = {
                                data: {finalizedHead, blocks: []},
                                bytes: 0,
                            }
                        } else {
                            buffer.data.finalizedHead = finalizedHead
                        }

                        for (let line of data.value) {
                            let block = JSON.parse(line) as B

                            buffer.bytes += line.length
                            buffer.data.blocks.push(block)

                            fromBlock = block.header.number + 1
                        }

                        if (buffer.bytes >= minBytes) {
                            ready()
                        }

                        if (isReady) {
                            putFuture = putFuture?.resolve() ?? undefined
                        }

                        if (buffer.bytes >= maxBytes) {
                            await waitPull()
                        }
                    }

                    destroyIdleInterval()

                    if (buffer != null) {
                        ready()
                        putFuture = putFuture?.resolve() ?? undefined
                    }
                }
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
            reader?.cancel().catch(() => {})
            abortSignal.removeEventListener('abort', abort)
            destroyIdleInterval()
        }
    }

    return new ReadableStream({
        start(controller) {
            ingest()
                .catch((err) => controller.error(err))
                .finally(() => close())
        },
        async pull(controller) {
            await waitPut()

            if (buffer != null) {
                controller.enqueue(buffer.data)
                isReady = false
                buffer = undefined
                pullFuture = pullFuture?.resolve() ?? undefined
                lastPullTimestamp = Date.now()
            }

            if (isClosed) {
                controller.close()
            }
        },
        cancel(reason) {
            abortStream.abort(reason)
        },
    })
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
                if (lines.length === 1) {
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
                // NOTE: not needed according to the spec, but done the same way in nodejs sources
                controller.terminate()
            },
        })
    }
}
