
export function addTimeout<T>(
    promise: Promise<T>,
    ms?: number,
    onTimeout?: () => Error | undefined | void
): Promise<T> {
    if (!ms) return promise

    return new Promise((resolve, reject) => {
        let timer = setTimeout(() => {
            let err = onTimeout?.() || new TimeoutError(ms)
            reject(err)
        }, ms)

        promise.finally(() => clearTimeout(timer)).then(resolve, reject)
    })
}


export function addStreamTimeout<T>(stream: ReadableStream<T>, ms: number, onTimeout?: () => Error | undefined | void): ReadableStream<T> {
    if (!ms) return stream

    let reader = stream.getReader()
    return new ReadableStream({
        pull: async (c) => {
            try {
                let data = await addTimeout(reader.read(), ms, onTimeout)

                if (data.done) {
                    c.close()
                } else {
                    c.enqueue(data.value)
                }
            } catch (e) {
                c.error(e)
                await reader.cancel()
            }
        },
        cancel: async (reason) => {
            await reader.cancel(reason)
        },
    })
}

export class TimeoutError extends Error {
    constructor(ms: number) {
        super(`timed out after ${ms} ms`)
    }

    get name(): string {
        return 'TimeoutError'
    }
}
