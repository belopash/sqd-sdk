import type {
    AddPrefix,
    ConditionalKeys,
    ExcludeUndefined,
    RemoveEmptyObjects,
    RemovePrefix,
    // Schema,
    Select,
    Selector,
    Simplify,
} from '@subsquid/util-types'

export type Bytes = string & {}
export type Bytes8 = string & {}
export type Bytes20 = string & {}
export type Bytes32 = string & {}

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
export type BlockHeaderFieldSelection = Simplify<
    Selector<keyof _BlockHeader> & {
        hash: true
        number: true
    }
>
export type BlockHeader<T extends keyof BlockHeaderFieldSelection = keyof BlockHeaderFieldSelection> = Simplify<
    Select<_BlockHeader, T>
>

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
export type TransactionFieldSelection = Selector<keyof _Transaction>
export type Transaction<T extends keyof TransactionFieldSelection = keyof TransactionFieldSelection> = Simplify<
    Select<_Transaction, T>
>

type _Log = {
    logIndex: number
    transactionIndex: number
    transactionHash: Bytes32
    address: Bytes20
    data: Bytes
    topics: Bytes32[]
}
export type LogFieldSelection = Selector<keyof _Log>
export type Log<T extends keyof LogFieldSelection = keyof LogFieldSelection> = Simplify<Select<_Log, T>>

type TraceBase = {
    type: string
    transactionIndex: number
    traceAddress: number[]
    subtraces: number
    error: string | null
    revertReason?: string
}

type _TraceCreateAction = {
    from: Bytes20
    value: bigint
    gas: bigint
    init: Bytes
}
export type TraceCreateActionFieldSelection = Selector<keyof _TraceCreateAction>
export type TraceCreateAction<T extends keyof TraceCreateActionFieldSelection = keyof TraceCreateActionFieldSelection> =
    Simplify<Select<_TraceCreateAction, T>>

type _TraceCreateResult = {
    gasUsed: bigint
    code: Bytes
    address: Bytes20
}
export type TraceCreateResultFieldSelection = Selector<keyof _TraceCreateResult>
export type TraceCreateResult<T extends keyof TraceCreateResultFieldSelection = keyof TraceCreateResultFieldSelection> =
    Simplify<Select<_TraceCreateResult, T>>

type _TraceCreate = TraceBase & {
    type: 'create'
}
export type TraceCreateFieldSelection = Selector<keyof _TraceCreate> &
    TraceCreateActionFieldSelection &
    Selector<AddPrefix<'result', keyof TraceCreateResultFieldSelection>>
export type TraceCreate<T extends keyof TraceCreateFieldSelection = keyof TraceCreateFieldSelection> = Simplify<
    RemoveEmptyObjects<
        Select<_TraceCreate, T> & {
            action: TraceCreateAction<Extract<T, keyof TraceCreateActionFieldSelection>>
            result?: TraceCreateResult<Extract<RemovePrefix<'result', T>, keyof TraceCreateResultFieldSelection>>
        }
    >
>

type _TraceCallAction = {
    callType: string
    from: Bytes20
    to: Bytes20
    value?: bigint
    gas: bigint
    input: Bytes
    sighash: Bytes
}
export type TraceCallActionFieldSelection = Selector<keyof _TraceCallAction>
export type TraceCallAction<T extends keyof TraceCallActionFieldSelection = keyof TraceCallActionFieldSelection> =
    Simplify<Select<_TraceCallAction, T>>

type _TraceCallResult = {
    gasUsed: bigint
    output: Bytes
}
export type TraceCallResultFieldSelection = Selector<keyof _TraceCallResult>
export type TraceCallResult<T extends keyof TraceCallResultFieldSelection = keyof TraceCallResultFieldSelection> =
    Simplify<Select<_TraceCallResult, T>>

type _TraceCall = TraceBase & {
    type: 'call'
}
export type TraceCallFieldSelection = Selector<keyof _TraceCall> &
    TraceCallActionFieldSelection &
    Selector<AddPrefix<'result', keyof TraceCallResultFieldSelection>>
export type TraceCall<T extends keyof TraceCallFieldSelection = keyof TraceCallFieldSelection> = Simplify<
    RemoveEmptyObjects<
        Select<_TraceCall, T> & {
            action: TraceCallAction<Extract<T, keyof TraceCallActionFieldSelection>>
            result?: TraceCallResult<Extract<RemovePrefix<'result', T>, keyof TraceCallResultFieldSelection>>
        }
    >
>

type _TraceSuicideAction = {
    address: Bytes20
    refundAddress: Bytes20
    balance: bigint
}
export type TraceSuicideActionFieldSelection = Selector<keyof _TraceSuicideAction>
export type TraceSuicideAction<
    T extends keyof TraceSuicideActionFieldSelection = keyof TraceSuicideActionFieldSelection
> = Simplify<Select<_TraceSuicideAction, T>>

type _TraceSuicide = TraceBase & {
    type: 'suicide'
}
export type TraceSuicideFieldSelection = Selector<keyof _TraceSuicide> & TraceSuicideActionFieldSelection
export type TraceSuicide<T extends keyof TraceSuicideFieldSelection = keyof TraceSuicideFieldSelection> = Simplify<
    RemoveEmptyObjects<
        Select<_TraceSuicide, T> & {
            action: TraceSuicideAction<Extract<T, keyof TraceSuicideActionFieldSelection>>
        }
    >
>

type _TraceRewardAction = {
    author: Bytes20
    value: bigint
    type: string
}
export type TraceRewardActionFieldSelection = Selector<keyof _TraceRewardAction>
export type TraceRewardAction<T extends keyof TraceRewardActionFieldSelection = keyof TraceRewardActionFieldSelection> =
    Simplify<Select<_TraceRewardAction, T>>

type _TraceReward = TraceBase & {
    type: 'reward'
}
export type TraceRewardFieldSelection = Selector<keyof _TraceReward> & TraceRewardActionFieldSelection
export type TraceReward<T extends keyof TraceRewardFieldSelection = keyof TraceRewardFieldSelection> = Simplify<
    RemoveEmptyObjects<
        Select<_TraceReward, T> & {
            action: TraceRewardAction<Extract<T, keyof TraceRewardActionFieldSelection>>
        }
    >
>

export type TraceFieldSelection = Selector<keyof TraceBase> &
    Selector<AddPrefix<'create', Exclude<keyof TraceCreateFieldSelection, keyof TraceBase>>> &
    Selector<AddPrefix<'call', Exclude<keyof TraceCallFieldSelection, keyof TraceBase>>> &
    Selector<AddPrefix<'suicide', Exclude<keyof TraceSuicideFieldSelection, keyof TraceBase>>> &
    Selector<AddPrefix<'reward', Exclude<keyof TraceRewardFieldSelection, keyof TraceBase>>>
export type Trace<T extends keyof TraceFieldSelection = keyof TraceFieldSelection> =
    | TraceCreate<Extract<T, keyof TraceBase> | Extract<RemovePrefix<'create', T>, keyof TraceCreateFieldSelection>>
    | TraceCall<Extract<T, keyof TraceBase> | Extract<RemovePrefix<'call', T>, keyof TraceCallFieldSelection>>
    | TraceSuicide<Extract<T, keyof TraceBase> | Extract<RemovePrefix<'suicide', T>, keyof TraceSuicideFieldSelection>>
    | TraceReward<Extract<T, keyof TraceBase> | Extract<RemovePrefix<'reward', T>, keyof TraceRewardFieldSelection>>

type A = Trace

type _StateDiffBase = {
    transactionIndex: number
    address: Bytes20
    key: 'balance' | 'code' | 'nonce' | Bytes32
    kind: string
    prev?: unknown
    next?: unknown
}

export type StateDiffFieldSelection = Selector<keyof _StateDiffBase>

type _StateDiffNoChange = Simplify<
    _StateDiffBase & {
        kind: '='
        prev?: null
        next?: null
    }
>

export type StateDiffNoChange<F extends StateDiffFieldSelection | true = true> = Simplify<
    Select<_StateDiffNoChange, ConditionalKeys<F>, true>
>

type _StateDiffAdd = Simplify<
    _StateDiffBase & {
        kind: '+'
        prev?: null
        next: Bytes
    }
>

export type StateDiffAdd<F extends StateDiffFieldSelection | true = true> = Simplify<
    Select<_StateDiffAdd, ConditionalKeys<F>, true>
>

type _StateDiffChange = Simplify<
    StateDiffBase & {
        kind: '*'
        prev: Bytes
        next: Bytes
    }
>

export type StateDiffDelete = Simplify<
    StateDiffBase & {
        kind: '-'
        prev: Bytes
        next?: null
    }
>

export type StateDiff = StateDiffNoChange | StateDiffAdd | StateDiffChange | StateDiffDelete

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
    logs: LogsRequest[]
    transactions: TransactionRequest[]
    traces: TraceRequest[]
    stateDiffs: StateDiffRequest[]
    includeAllBlocks: boolean
}>

export type EvmResponse<Q extends EvmQuery> = Simplify<{
    header: BlockHeader<Q['fields']['block']>
    logs: Log<ExcludeUndefined<Q['fields']['log']>>[]
    transactions: Transaction<ExcludeUndefined<Q['fields']['transaction']>>[]
    traces: Simplify<Select<Trace, ConditionalKeys<Q['fields']['trace']>>, true>[]
    stateDiffs: Simplify<Select<StateDiff, ConditionalKeys<Q['fields']['stateDiff']>>, true>[]
}>
