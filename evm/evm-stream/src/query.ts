import {applyRangeBound, mergeRangeRequests, Range, RangeRequest} from '@subsquid/util-internal-range'

import {
    DataRequest,
    LogRequest,
    TransactionRequest,
    TraceRequest,
    StateDiffRequest,
    FieldSelection,
} from '@subsquid/portal-client/lib/query/evm'

export * from '@subsquid/portal-client/lib/query/evm'

export type RequestOptions<R> = {range?: Range; request: R}
export type LogRequestOptions = RequestOptions<LogRequest>
export type TransactionRequestOptions = RequestOptions<TransactionRequest>
export type TraceRequestOptions = RequestOptions<TraceRequest>
export type StateDiffRequestOptions = RequestOptions<StateDiffRequest>

export type EvmQueryOptions<F extends FieldSelection> = {
    fields: F
    requests: RangeRequest<DataRequest>[]
}

export class EvmQueryBuilder<F extends FieldSelection = {block: {number: true; hash: true}}> {
    private range: Range = {from: 0}
    private requests: RangeRequest<DataRequest>[] = []
    private fields: F = {
        block: {number: true, hash: true},
    } as F

    addLog(options: LogRequestOptions): this {
        this.requests.push({
            range: options.range ?? {from: 0},
            request: {
                logs: [mapRequest(options)],
            },
        })
        return this
    }

    addTransaction(options: TransactionRequestOptions): this {
        this.requests.push({
            range: options.range ?? {from: 0},
            request: {
                transactions: [mapRequest(options)],
            },
        })
        return this
    }

    addTrace(options: TraceRequestOptions): this {
        this.requests.push({
            range: options.range ?? {from: 0},
            request: {
                traces: [mapRequest(options)],
            },
        })
        return this
    }

    addStateDiff(options: StateDiffRequestOptions): this {
        this.requests.push({
            range: options.range ?? {from: 0},
            request: {
                stateDiffs: [mapRequest(options)],
            },
        })
        return this
    }

    setRange(range: Range): this {
        this.range = range
        return this
    }

    setFields<F extends FieldSelection>(fields: F): EvmQueryBuilder<F> {
        this.fields = fields as any
        return this as any
    }

    build(): EvmQueryOptions<F> {
        let requests = mergeRangeRequests(this.requests, (a, b) => {
            let res: DataRequest = {}
            res.transactions = concatRequestLists(a.transactions, b.transactions)
            res.logs = concatRequestLists(a.logs, b.logs)
            res.traces = concatRequestLists(a.traces, b.traces)
            res.stateDiffs = concatRequestLists(a.stateDiffs, b.stateDiffs)
            if (a.includeAllBlocks || b.includeAllBlocks) {
                res.includeAllBlocks = true
            }
            return res
        })

        return {
            fields: this.fields,
            requests: applyRangeBound(requests, this.range),
        }
    }
}

function concatRequestLists<T extends object>(a?: T[], b?: T[]): T[] | undefined {
    let result: T[] = []
    if (a) {
        result.push(...a)
    }
    if (b) {
        result.push(...b)
    }
    return result.length == 0 ? undefined : result
}

function mapRequest<T>(options: RequestOptions<T>): T {
    let req = {...options.request}
    for (let key in req) {
        let val = (req as any)[key]
        if (Array.isArray(val)) {
            ;(req as any)[key] = val.map((s) => {
                return typeof s == 'string' ? s.toLowerCase() : s
            })
        }
    }
    return req
}
