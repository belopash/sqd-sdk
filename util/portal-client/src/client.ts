import {HttpClient, HttpBodyTimeoutError} from '@subsquid/http-client'
import {createFuture, Future, unexpectedCase, wait, withErrorContext} from '@subsquid/util-internal'

export type PortalQuery = EvmQuery

export type PortalResponse<Q extends PortalQuery> = Q extends EvmQuery ? EvmResponse<Q> : any

export type PortalClientOptions = {
    url: string
    http?: HttpClient

    minBytes?: number
    maxBytes?: number
    maxIdleTime?: number
    maxWaitTime?: number

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

        return createReadablePortalStream<B>(
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

function createReadablePortalStream<B extends Block>(
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
    let state: 'open' | 'failed' | 'closed' = 'open'
    let error: unknown

    let readyFuture: Future<void> = createFuture()
    let takeFuture: Future<void> = createFuture()
    let putFuture: Future<void> = createFuture()

    let lastChunkTimestamp = Date.now()
    let idleInterval: ReturnType<typeof setInterval> | undefined
    let waitTimeout: ReturnType<typeof setTimeout> | undefined

    async function take() {
        if (state === 'failed') {
            throw error
        }

        createWaitTimeout()
        await readyFuture.promise()
        await putFuture.promise()

        let value = buffer?.data
        buffer = undefined
        takeFuture.resolve()

        if (state === 'closed') {
            return {value, done: value == null}
        } else {
            takeFuture = createFuture()
            putFuture = createFuture()
            readyFuture = createFuture()

            return {value, done: false}
        }
    }

    function close() {
        if (state !== 'open') return
        state = 'closed'
        readyFuture.resolve()
        putFuture.resolve()
        takeFuture.resolve()
    }

    function fail(err: unknown) {
        if (state !== 'open') return
        state = 'failed'
        error = err
        readyFuture.resolve()
        putFuture.resolve()
        takeFuture.resolve()
    }

    function createIdleInterval() {
        clearInterval(idleInterval)

        let isTriggered = false
        idleInterval = setInterval(() => {
            if (isTriggered) return
            if (Date.now() - lastChunkTimestamp >= maxIdleTime) {
                isTriggered = true
                readyFuture.resolve()
            }
        }, Math.ceil(maxIdleTime / 3))
        readyFuture.promise().finally(destroyIdleInterval)
    }

    function destroyIdleInterval() {
        clearInterval(idleInterval)
        idleInterval = undefined
    }

    function createWaitTimeout() {
        clearTimeout(waitTimeout)

        waitTimeout = setTimeout(() => readyFuture.resolve(), maxWaitTime)
        readyFuture.promise().finally(destroyWaitTimeout)
    }

    function destroyWaitTimeout() {
        clearTimeout(waitTimeout)
        waitTimeout = undefined
    }

    async function ingest() {
        let abortSignal = abortStream.signal
        let {fromBlock, toBlock = Infinity} = query

        try {
            let reader: ReadableStreamDefaultReader<string[]> | undefined

            try {
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
                            let data = await withAbort(reader.read(), abortSignal)
                            if (data.done) break
                            if (data.value.length == 0) continue

                            lastChunkTimestamp = Date.now()
                            if (idleInterval == null) {
                                createIdleInterval()
                            }

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
                                readyFuture.resolve()
                            }

                            putFuture.resolve()

                            if (buffer.bytes >= maxBytes) {
                                await withAbort(takeFuture.promise(), abortSignal)
                            }
                        }

                        destroyIdleInterval()
                        if (buffer != null) {
                            readyFuture.resolve()
                        }
                    }
                }
            } catch (err) {
                if (err instanceof HttpBodyTimeoutError) {
                    // ignore
                } else {
                    throw err
                }
            } finally {
                reader?.cancel().catch(() => {})
            }
        } catch (err) {
            if (abortSignal.aborted) {
                // ignore
            } else {
                throw err
            }
        }
    }

    return new ReadableStream({
        start() {
            ingest().then(close, fail)
        },
        async pull(controller) {
            try {
                let result = await take()
                if (result.done) {
                    controller.close()
                } else {
                    controller.enqueue(result.value)
                }
            } catch (err) {
                controller.error(err)
            }
        },
        cancel(reason) {
            abortStream.abort(reason)
        },
    })
}

function withAbort<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
    return new Promise<T>((resolve, reject) => {
        if (signal.aborted) {
            reject(signal.reason || new Error('Aborted'))
        }

        const abort = () => {
            reject(signal.reason || new Error('Aborted'))
        }

        signal.addEventListener('abort', abort)

        promise.then(resolve, reject).finally(() => {
            signal.removeEventListener('abort', abort)
        })
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
