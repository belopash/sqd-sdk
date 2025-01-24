import type {Range} from '@subsquid/util-internal-range'
import type {Bytes, Bytes20, Bytes32} from '@subsquid/util-types'
import type {FieldSelection} from './data'

export type EvmQuery<F extends FieldSelection> = {
    fields: F
    ranges: EvmQueryRange[]
}

export type EvmQueryRange = {
    range: Range
    request: DataRequest
}

export interface DataRequest {
    includeAllBlocks?: boolean
    logs?: LogRequest[]
    transactions?: TransactionRequest[]
    traces?: TraceRequest[]
    stateDiffs?: StateDiffRequest[]
}

export interface LogRequest {
    address?: Bytes20[]
    topic0?: Bytes32[]
    topic1?: Bytes32[]
    topic2?: Bytes32[]
    topic3?: Bytes32[]
    include: {
        transaction?: boolean
        transactionTraces?: boolean
        transactionLogs?: boolean
        transactionStateDiffs?: boolean
    }
}

export interface TransactionRequest {
    to?: Bytes20[]
    from?: Bytes20[]
    sighash?: Bytes[]
    type?: number[]
    include: {
        logs?: boolean
        traces?: boolean
        stateDiffs?: boolean
    }
}

export interface TraceRequest {
    type?: string[]
    createFrom?: Bytes20[]
    callTo?: Bytes20[]
    callFrom?: Bytes20[]
    callSighash?: Bytes[]
    suicideRefundAddress?: Bytes[]
    rewardAuthor?: Bytes20[]
    include: {
        transaction?: boolean
        transactionLogs?: boolean
        subtraces?: boolean
        parents?: boolean
    }
}

export interface StateDiffRequest {
    address?: Bytes20[]
    key?: Bytes[]
    kind?: string[]
    include: {
        transaction?: boolean
    }
}