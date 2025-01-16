import {applyRangeBound, mergeRangeRequests, Range} from '@subsquid/util-internal-range'
import {
    DataRequest,
    EvmQuery,
    EvmQueryRange,
    LogRequest,
    StateDiffRequest,
    TraceRequest,
    TransactionRequest,
} from './data/request'

export interface RequestOptions {
    range: Range
}

export interface LogRequestOptions extends LogRequest, RequestOptions {}
export interface TransactionRequestOptions extends TransactionRequest, RequestOptions {}
export interface TraceRequestOptions extends TraceRequest, RequestOptions {}
export interface StateDiffRequestOptions extends StateDiffRequest, RequestOptions {}

export class EvmQueryBuilder {
    private range: Range = {from: 0}
    private ranges: EvmQueryRange[] = []

    addLog(options: LogRequestOptions): this {
        this.ranges.push({
            range: options.range,
            request: {
                logs: [mapRequest(options)],
            },
        })
        return this
    }

    addTransaction(options: TransactionRequestOptions): this {
        this.ranges.push({
            range: options.range,
            request: {
                transactions: [mapRequest(options)],
            },
        })
        return this
    }

    addTrace(options: TraceRequestOptions): this {
        this.ranges.push({
            range: options.range,
            request: {
                traces: [mapRequest(options)],
            },
        })
        return this
    }

    addStateDiff(options: StateDiffRequestOptions): this {
        this.ranges.push({
            range: options.range,
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

    build(): EvmQuery {
        let ranges = mergeRangeRequests(this.ranges, (a, b) => {
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
            ranges: applyRangeBound(ranges, this.range),
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

function mapRequest<T extends RequestOptions>(req: T): Omit<T, 'range'> {
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
