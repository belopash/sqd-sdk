import type {
    AddPrefix,
    Bytes,
    Bytes20,
    Bytes32,
    Bytes8,
    ExcludeUndefined,
    RemoveEmptyObjects,
    RemoveKeysPrefix,
    Select,
    Selector,
    Simplify,
} from '@subsquid/util-types'

export type BlockHeaderFields = {
    number: number
    hash: Bytes32
    parentHash: Bytes32
    timestamp: number
    transactionsRoot: Bytes32
    receiptsRoot: Bytes32
    stateRoot: Bytes32
    logsBloom: Bytes
    sha3Uncles: Bytes32
    extraData: Bytes
    miner: Bytes20
    nonce: Bytes8
    mixHash: Bytes
    size: bigint
    gasLimit: bigint
    gasUsed: bigint
    difficulty: bigint
    totalDifficulty: bigint
    baseFeePerGas: bigint
    blobGasUsed: bigint
    excessBlobGas: bigint
    l1BlockNumber?: number
}

export type TransactionFields = {
    transactionIndex: number
    hash: Bytes32
    nonce: number
    from: Bytes20
    to?: Bytes20
    input: Bytes
    value: bigint
    gas: bigint
    gasPrice: bigint
    maxFeePerGas?: bigint
    maxPriorityFeePerGas?: bigint
    v: bigint
    r: Bytes32
    s: Bytes32
    yParity?: number
    chainId?: number
    sighash?: Bytes8
    contractAddress?: Bytes20
    gasUsed: bigint
    cumulativeGasUsed: bigint
    effectiveGasPrice: bigint
    type: number
    status: number
    blobVersionedHashes: Bytes[]

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
    type: string
    transactionIndex: number
    traceAddress: number[]
    subtraces: number
    error: string | null
    revertReason?: string
}

export type TraceCreateFields = TraceBaseFields & {
    type: 'create'
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
}

export type TraceSuicideActionFields = {
    address: Bytes20
    refundAddress: Bytes20
    balance: bigint
}

export type TraceRewardFields = TraceBaseFields & {
    type: 'reward'
}

export type TraceRewardActionFields = {
    author: Bytes20
    value: bigint
    type: string
}

export type StateDiffBaseFields = {
    transactionIndex: number
    address: Bytes20
    key: 'balance' | 'code' | 'nonce' | Bytes32
    kind: string
    prev?: unknown
    next?: unknown
}

export type StateDiffAddFields = StateDiffBaseFields & {
    kind: '+'
    prev?: null
    next: Bytes
}

export type StateDiffNoChangeFields = StateDiffBaseFields & {
    kind: '='
    prev?: null
    next?: null
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

type Trues<T> = {[K in keyof T]-?: true}

export type BlockHeaderFieldSelection = Simplify<Selector<keyof BlockHeaderFields>>
export type BlockHeader<T extends BlockHeaderFieldSelection = Trues<BlockHeaderFieldSelection>> = Simplify<
    Select<BlockHeaderFields, T>
>

export type TransactionFieldSelection = Selector<keyof TransactionFields>
export type Transaction<T extends TransactionFieldSelection = Trues<TransactionFieldSelection>> = Simplify<
    Select<TransactionFields, T>
>

export type LogFieldSelection = Selector<keyof LogFields>
export type Log<T extends LogFieldSelection = Trues<LogFieldSelection>> = Simplify<Select<LogFields, T>>

export type TraceFieldSelection = Selector<
    | keyof TraceBaseFields
    | AddPrefix<'create', keyof TraceCreateActionFields>
    | AddPrefix<'createResult', keyof TraceCreateResultFields>
    | AddPrefix<'call', keyof TraceCallActionFields>
    | AddPrefix<'callResult', keyof TraceCallResultFields>
    | AddPrefix<'suicide', keyof TraceSuicideActionFields>
    | AddPrefix<'reward', keyof TraceRewardActionFields>
>

export type TraceCreateAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceCreateActionFields, RemoveKeysPrefix<'create', F>>
>

export type TraceCreateResult<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceCreateResultFields, RemoveKeysPrefix<'createResult', F>>
>

export type TraceCallAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceCallActionFields, RemoveKeysPrefix<'call', F>>
>

export type TraceCallResult<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceCallResultFields, RemoveKeysPrefix<'callResult', F>>
>

export type TraceSuicideAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceSuicideActionFields, RemoveKeysPrefix<'suicide', F>>
>

export type TraceRewardAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceRewardActionFields, RemoveKeysPrefix<'reward', F>>
>

export type TraceCreate<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceCreateFields, F> & RemoveEmptyObjects<{action: TraceCreateAction<F>; result?: TraceCreateResult<F>}>
>

export type TraceCall<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceCallFields, F> & RemoveEmptyObjects<{action: TraceCallAction<F>; result?: TraceCallResult<F>}>
>

export type TraceSuicide<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceSuicideFields, F> & RemoveEmptyObjects<{action: TraceSuicideAction<F>}>
>

export type TraceReward<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<TraceRewardFields, F> & RemoveEmptyObjects<{action: TraceRewardAction<F>}>
>

export type Trace<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = F extends any
    ? TraceCreate<F> | TraceCall<F> | TraceSuicide<F> | TraceReward<F>
    : never

export type StateDiffFieldSelection = Selector<keyof StateDiffBaseFields>

export type StateDiffNoChange<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<StateDiffNoChangeFields, F>
>

export type StateDiffAdd<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<StateDiffAddFields, F>
>

export type StateDiffChange<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<StateDiffChangeFields, F>
>

export type StateDiffDelete<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<StateDiffDeleteFields, F>
>

export type StateDiff<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = F extends any
    ? StateDiffNoChange<F> | StateDiffAdd<F> | StateDiffChange<F> | StateDiffDelete<F>
    : never

export type FieldSelection = {
    block: BlockHeaderFieldSelection
    transaction?: TransactionFieldSelection
    log?: LogFieldSelection
    trace?: TraceFieldSelection
    stateDiff?: StateDiffFieldSelection
}

export type LogRequest = {
    address?: Bytes20[]
    topic0?: Bytes32[]
    topic1?: Bytes32[]
    topic2?: Bytes32[]
    topic3?: Bytes32[]
    transaction?: boolean
    transactionTraces?: boolean
    transactionLogs?: boolean
    transactionStateDiffs?: boolean
}

export type TransactionRequest = {
    to?: Bytes20[]
    from?: Bytes20[]
    sighash?: Bytes[]
    type?: number[]
    logs?: boolean
    traces?: boolean
    stateDiffs?: boolean
}

export type TraceRequest = {
    type?: string[]
    createFrom?: Bytes20[]
    callTo?: Bytes20[]
    callFrom?: Bytes20[]
    callSighash?: Bytes[]
    suicideRefundAddress?: Bytes[]
    rewardAuthor?: Bytes20[]
    transaction?: boolean
    transactionLogs?: boolean
    subtraces?: boolean
    parents?: boolean
}

export type StateDiffRequest = {
    address?: Bytes20[]
    key?: Bytes[]
    kind?: string[]
    transaction?: boolean
}

export type DataRequest = {
    logs?: LogRequest[]
    transactions?: TransactionRequest[]
    traces?: TraceRequest[]
    stateDiffs?: StateDiffRequest[]
    includeAllBlocks?: boolean
}

export type FinalizedQuery = Simplify<
    {
        type: 'evm'
        fromBlock?: number
        toBlock?: number
        fields: FieldSelection
    } & DataRequest
>

export type BlockData<F extends FieldSelection> = Simplify<{
    header: BlockHeader<F['block']>
    logs?: Log<ExcludeUndefined<F['log']>>[]
    transactions?: Transaction<ExcludeUndefined<F['transaction']>>[]
    traces?: Trace<ExcludeUndefined<F['trace']>>[]
    stateDiffs?: StateDiff<ExcludeUndefined<F['stateDiff']>>[]
}>

export type Response<Q extends FinalizedQuery> = BlockData<Q['fields']>
