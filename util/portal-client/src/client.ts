import {HttpClient} from '@subsquid/http-client'
import {createFuture, Future, Throttler, unexpectedCase, wait, withErrorContext} from '@subsquid/util-internal'
import {HashAndNumber, PortalQuery, PortalResponse} from './query'

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

export interface PortalStreamData<B> {
    finalizedHead: HashAndNumber
    blocks: B[]
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

    getFinalizedQuery<Q extends PortalQuery = PortalQuery, R extends PortalResponse<Q> = PortalResponse<Q>>(
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

    getFinalizedStream<Q extends PortalQuery = PortalQuery, R extends PortalResponse<Q> = PortalResponse<Q>>(
        query: Q,
        options?: PortalStreamOptions
    ): ReadableStream<PortalStreamData<R>> {
        let {
            headPollInterval = this.headPollInterval,
            minBytes = this.minBytes,
            maxBytes = this.maxBytes,
            maxIdleTime = this.maxIdleTime,
            maxWaitTime = this.maxWaitTime,
            request = {},
            stopOnHead = false,
        } = options ?? {}

        let top = new Throttler(() => this.getFinalizedHeight(request), 20_000)
        return createReadablePortalStream(
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
                // NOTE: we emulate the same behavior as will be implemented for hot blocks stream,
                // but unfortunately we don't have any information about finalized block hash at the moment
                let finalizedHead = {
                    number: await top.get(),
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

function createReadablePortalStream<
    Q extends PortalQuery = PortalQuery,
    R extends PortalResponse<Q> = PortalResponse<Q>
>(
    query: Q,
    options: Required<PortalStreamOptions>,
    requestStream: (
        query: Q,
        options?: PortalRequestOptions
    ) => Promise<{finalizedHead: HashAndNumber; stream: ReadableStream<string[]>} | undefined>
): ReadableStream<PortalStreamData<R>> {
    let {headPollInterval, stopOnHead, maxBytes, minBytes, request, maxIdleTime, maxWaitTime} = options
    maxBytes = Math.max(maxBytes, minBytes)

    let abortStream = new AbortController()

    let buffer: {data: PortalStreamData<R>; bytes: number} | undefined
    let state: 'open' | 'failed' | 'closed' = 'open'
    let error: unknown

    let readyFuture: Future<void> = createFuture()
    let takeFuture: Future<void> = createFuture()
    let putFuture: Future<void> = createFuture()

    async function take() {
        let waitTimeout = setTimeout(() => {
            readyFuture.resolve()
        }, maxWaitTime)
        readyFuture.promise().finally(() => clearTimeout(waitTimeout))

        await Promise.all([readyFuture.promise(), putFuture.promise()])

        if (state === 'failed') {
            throw error
        }

        let value = buffer?.data
        buffer = undefined

        takeFuture.resolve()

        if (state === 'closed') {
            return {value, done: value == null}
        } else {
            if (value == null) {
                throw new Error('buffer is empty')
            }

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

    async function ingest() {
        let abortSignal = abortStream.signal
        let {fromBlock = 0, toBlock = Infinity} = query

        try {
            while (true) {
                if (abortSignal.aborted) break
                if (fromBlock > toBlock) break

                let reader: ReadableStreamDefaultReader<string[]> | undefined

                let lastChunkTimestamp = Date.now()
                let idleInterval: ReturnType<typeof setInterval> | undefined

                try {
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
                                idleInterval = setInterval(() => {
                                    if (Date.now() - lastChunkTimestamp >= maxIdleTime) {
                                        readyFuture.resolve()
                                    }
                                }, Math.ceil(maxIdleTime / 3))
                                readyFuture.promise().finally(() => clearInterval(idleInterval))
                                takeFuture.promise().finally(() => (idleInterval = undefined))
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
                                let block = JSON.parse(line) as R

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
                    }

                    if (buffer != null) {
                        readyFuture.resolve()
                    }
                } finally {
                    reader?.cancel().catch(() => {})
                    clearInterval(idleInterval)
                }
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
