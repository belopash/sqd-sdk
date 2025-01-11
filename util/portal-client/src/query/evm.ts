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

type _BlockHeader = {
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

type _Transaction = {
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

type _Log = {
    logIndex: number
    transactionIndex: number
    transactionHash: Bytes32
    address: Bytes20
    data: Bytes
    topics: Bytes32[]
}

type _TraceBase = {
    type: string
    transactionIndex: number
    traceAddress: number[]
    subtraces: number
    error: string | null
    revertReason?: string
}

type _TraceCreate = _TraceBase & {
    type: 'create'
}

type _TraceCreateAction = {
    from: Bytes20
    value: bigint
    gas: bigint
    init: Bytes
}

type _TraceCreateResult = {
    gasUsed: bigint
    code: Bytes
    address: Bytes20
}

type _TraceCall = _TraceBase & {
    type: 'call'
}

type _TraceCallAction = {
    callType: string
    from: Bytes20
    to: Bytes20
    value?: bigint
    gas: bigint
    input: Bytes
    sighash: Bytes
}

type _TraceCallResult = {
    gasUsed: bigint
    output: Bytes
}

type _TraceSuicide = _TraceBase & {
    type: 'suicide'
}

type _TraceSuicideAction = {
    address: Bytes20
    refundAddress: Bytes20
    balance: bigint
}

type _TraceReward = _TraceBase & {
    type: 'reward'
}

type _TraceRewardAction = {
    author: Bytes20
    value: bigint
    type: string
}

type _StateDiffBase = {
    transactionIndex: number
    address: Bytes20
    key: 'balance' | 'code' | 'nonce' | Bytes32
    kind: string
    prev?: unknown
    next?: unknown
}

type _StateDiffAdd = _StateDiffBase & {
    kind: '+'
    prev?: null
    next: Bytes
}

type _StateDiffNoChange = _StateDiffBase & {
    kind: '='
    prev?: null
    next?: null
}

type _StateDiffChange = _StateDiffBase & {
    kind: '*'
    prev: Bytes
    next: Bytes
}

type _StateDiffDelete = _StateDiffBase & {
    kind: '-'
    prev: Bytes
    next?: null
}

type Trues<T> = {[K in keyof T]-?: true}

export type BlockHeaderFieldSelection = Simplify<
    Selector<keyof _BlockHeader> & {
        hash: true
        number: true
    }
>
export type BlockHeader<T extends BlockHeaderFieldSelection = Trues<BlockHeaderFieldSelection>> = Simplify<
    Select<_BlockHeader, T>
>

export type TransactionFieldSelection = Selector<keyof _Transaction>
export type Transaction<T extends TransactionFieldSelection = Trues<TransactionFieldSelection>> = Simplify<
    Select<_Transaction, T>
>

export type LogFieldSelection = Selector<keyof _Log>
export type Log<T extends LogFieldSelection = Trues<LogFieldSelection>> = Simplify<Select<_Log, T>>

export type TraceFieldSelection = Selector<
    | keyof _TraceBase
    | AddPrefix<'create', keyof _TraceCreateAction>
    | AddPrefix<'createResult', keyof _TraceCreateResult>
    | AddPrefix<'call', keyof _TraceCallAction>
    | AddPrefix<'callResult', keyof _TraceCallResult>
    | AddPrefix<'suicide', keyof _TraceSuicideAction>
    | AddPrefix<'reward', keyof _TraceRewardAction>
>

export type TraceCreateAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceCreateAction, RemoveKeysPrefix<'create', F>>
>

export type TraceCreateResult<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceCreateResult, RemoveKeysPrefix<'createResult', F>>
>

export type TraceCallAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceCallAction, RemoveKeysPrefix<'call', F>>
>

export type TraceCallResult<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceCallResult, RemoveKeysPrefix<'callResult', F>>
>

export type TraceSuicideAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceSuicideAction, RemoveKeysPrefix<'suicide', F>>
>

export type TraceRewardAction<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceRewardAction, RemoveKeysPrefix<'reward', F>>
>

export type TraceCreate<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceCreate, F> & RemoveEmptyObjects<{action: TraceCreateAction<F>; result?: TraceCreateResult<F>}>
>

export type TraceCall<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceCall, F> & RemoveEmptyObjects<{action: TraceCallAction<F>; result?: TraceCallResult<F>}>
>

export type TraceSuicide<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceSuicide, F> & RemoveEmptyObjects<{action: TraceSuicideAction<F>}>
>

export type TraceReward<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = Simplify<
    Select<_TraceReward, F> & RemoveEmptyObjects<{action: TraceRewardAction<F>}>
>

export type Trace<F extends TraceFieldSelection = Trues<TraceFieldSelection>> = F extends any
    ? TraceCreate<F> | TraceCall<F> | TraceSuicide<F> | TraceReward<F>
    : never

export type StateDiffFieldSelection = Selector<keyof _StateDiffBase>

export type StateDiffNoChange<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<_StateDiffNoChange, F>
>

export type StateDiffAdd<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<_StateDiffAdd, F>
>

export type StateDiffChange<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<_StateDiffChange, F>
>

export type StateDiffDelete<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    Select<_StateDiffDelete, F>
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

export type LogsRequest = {
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

export type EvmQuery = Simplify<{
    type: 'evm'
    fromBlock?: number
    toBlock?: number
    fields: FieldSelection
    logs?: LogsRequest[]
    transactions?: TransactionRequest[]
    traces?: TraceRequest[]
    stateDiffs?: StateDiffRequest[]
    includeAllBlocks?: boolean
}>

export type EvmResponse<Q extends EvmQuery> = Simplify<{
    header: BlockHeader<Q['fields']['block']>
    logs?: Log<ExcludeUndefined<Q['fields']['log']>>[]
    transactions?: Transaction<ExcludeUndefined<Q['fields']['transaction']>>[]
    traces?: Trace<ExcludeUndefined<Q['fields']['trace']>>[]
    stateDiffs?: StateDiff<ExcludeUndefined<Q['fields']['stateDiff']>>[]
}>
