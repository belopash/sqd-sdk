import type {Bytes32, Bytes8, Bytes, Bytes20} from '@subsquid/util-types'

export type BlockFields = {
    number: number
    hash: Bytes32
    parentHash: Bytes32
    nonce: Bytes8
    sha3Uncles: Bytes32
    logsBloom: Bytes
    transactionsRoot: Bytes32
    stateRoot: Bytes32
    receiptsRoot: Bytes32
    mixHash: Bytes
    miner: Bytes20
    difficulty: bigint
    totalDifficulty: bigint
    extraData: Bytes
    size: bigint
    gasLimit: bigint
    gasUsed: bigint
    timestamp: number
    baseFeePerGas: bigint
    l1BlockNumber: number

    // @deprecated
    height: number
}

export type TransactionFields = {
    transactionIndex: number
    sighash: Bytes
    hash: Bytes32
    from: Bytes20
    to?: Bytes20
    gas: bigint
    gasPrice: bigint
    maxFeePerGas?: bigint
    maxPriorityFeePerGas?: bigint
    input: Bytes
    nonce: number
    value: bigint
    v: bigint
    r: Bytes32
    s: Bytes32
    yParity?: number
    chainId?: number
}

export type TransactionReceiptFields = {
    transactionHash: Bytes32
    transactionIndex: number
    from: Bytes20
    gasUsed: bigint
    cumulativeGasUsed: bigint
    effectiveGasPrice: bigint
    contractAddress?: Bytes32
    type: number
    status: number
    l1Fee?: bigint
    l1FeeScalar?: number
    l1GasPrice?: bigint
    l1GasUsed?: bigint
    l1BlobBaseFee?: bigint
    l1BlobBaseFeeScalar?: number
    l1BaseFeeScalar?: number
}

export type LogFields = {
    logIndex: number
    transactionIndex: number
    transactionHash: Bytes32
    address: Bytes20
    data: Bytes
    topics: Bytes32[]
}

export type TraceBaseFields = {
    transactionIndex: number
    traceAddress: number[]
    subtraces: number
    error: string | null
    revertReason?: string
}

export type TraceCreateFields = TraceBaseFields & {
    type: 'create'
    action: TraceCreateActionFields
    result?: TraceCreateResultFields
}

export type TraceCreateActionFields = {
    from: Bytes20
    value: bigint
    gas: bigint
    init: Bytes
}

export type TraceCreateResultFields = {
    gasUsed: bigint
    code: Bytes
    address: Bytes20
}

export type TraceCallFields = TraceBaseFields & {
    type: 'call'
    action: TraceCallActionFields
    result?: TraceCallResultFields
}

export type TraceCallActionFields = {
    callType: string
    from: Bytes20
    to: Bytes20
    value?: bigint
    gas: bigint
    input: Bytes
    sighash: Bytes
}

export type TraceCallResultFields = {
    gasUsed: bigint
    output: Bytes
}

export type TraceSuicideFields = TraceBaseFields & {
    type: 'suicide'
    action: TraceSuicideActionFields
}

export type TraceSuicideActionFields = {
    address: Bytes20
    refundAddress: Bytes20
    balance: bigint
}

export type TraceRewardFields = TraceBaseFields & {
    type: 'reward'
    action: TraceRewardActionFields
}

export type TraceRewardActionFields = {
    author: Bytes20
    value: bigint
    type: string
}

export type TraceFields = TraceCreateFields | TraceCallFields | TraceSuicideFields | TraceRewardFields

export type StateDiffBaseFields = {
    transactionIndex: number
    address: Bytes20
    key: 'balance' | 'code' | 'nonce' | Bytes32
    kind: '=' | '+' | '*' | '-'
}

export type StateDiffNoChangeFields = StateDiffBaseFields & {
    kind: '='
    prev?: null
    next?: null
}

export type StateDiffAddFields = StateDiffBaseFields & {
    kind: '+'
    prev?: null
    next: Bytes
}

export type StateDiffChangeFields = StateDiffBaseFields & {
    kind: '*'
    prev: Bytes
    next: Bytes
}

export type StateDiffDeleteFields = StateDiffBaseFields & {
    kind: '-'
    prev: Bytes
    next?: null
}

export type StateDiffFields =
    | StateDiffAddFields
    | StateDiffChangeFields
    | StateDiffDeleteFields
    | StateDiffNoChangeFields