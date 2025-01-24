import type {MergeDefault, Select, Selector, Simplify} from '@subsquid/util-types'
import type {
    BlockFields,
    LogFields,
    StateDiffBaseFields,
    TraceBaseFields,
    TraceCallActionFields,
    TraceCallResultFields,
    TraceCallFields,
    TraceCreateFields,
    TraceCreateActionFields,
    TraceCreateResultFields,
    TraceRewardActionFields,
    TraceSuicideActionFields,
    TransactionFields,
    TransactionReceiptFields,
    StateDiffFields,
    TraceSuicideFields,
    TraceRewardFields,
} from './evm'

export type BlockRequiredFields = 'number' | 'hash' | 'parentHash'
export type TransactionRequiredFields = 'transactionIndex'
export type TransactionReceiptRequiredFields = 'transactionIndex'
export type LogRequiredFields = 'logIndex' | 'transactionIndex'
export type TraceRequiredFields = 'transactionIndex' | 'traceAddress' | 'type'
export type StateDiffRequiredFields = 'transactionIndex' | 'address' | 'key' | 'kind'

export type BlockFieldSelection = Selector<Exclude<keyof BlockFields, BlockRequiredFields>>
export type TransactionFieldSelection = Selector<Exclude<keyof TransactionFields, TransactionRequiredFields>>
export type TransactionReceiptFieldSelection = Selector<
    Exclude<keyof TransactionReceiptFields, TransactionReceiptRequiredFields>
>
export type LogFieldSelection = Selector<Exclude<keyof LogFields, LogRequiredFields>>
export type TraceCreateActionFieldSelection = Selector<keyof TraceCreateActionFields>
export type TraceCreateResultFieldSelection = Selector<keyof TraceCreateResultFields>
export type TraceCreateFieldSelection = Simplify<
    Selector<Exclude<keyof TraceCreateFields, TraceRequiredFields | 'action' | 'result'>> & {
        action?: TraceCreateActionFieldSelection
        result?: TraceCreateResultFieldSelection
    }
>
export type TraceCallActionFieldSelection = Selector<keyof TraceCallActionFields>
export type TraceCallResultFieldSelection = Selector<keyof TraceCallResultFields>
export type TraceCallFieldSelection = Simplify<
    Selector<Exclude<keyof TraceCallFields, TraceRequiredFields | 'action' | 'result'>> & {
        action?: TraceCallActionFieldSelection
        result?: TraceCallResultFieldSelection
    }
>
export type TraceRewardActionFieldSelection = Selector<keyof TraceRewardActionFields>
export type TraceRewardFieldSelection = Simplify<
    Selector<Exclude<keyof TraceRewardFields, TraceRequiredFields | 'action'>> & {
        action?: TraceRewardActionFieldSelection
    }
>
export type TraceSuicideActionFieldSelection = Selector<keyof TraceSuicideActionFields>
export type TraceSuicideFieldSelection = Simplify<
    Selector<Exclude<keyof TraceSuicideFields, TraceRequiredFields | 'action'>> & {
        action?: TraceSuicideActionFieldSelection
    }
>
export type TraceFieldSelection = Simplify<
    Selector<Exclude<keyof TraceBaseFields, TraceRequiredFields>> & {
        create?: TraceCreateFieldSelection
        call?: TraceCallFieldSelection
        reward?: TraceRewardFieldSelection
        suicide?: TraceSuicideFieldSelection
    }
>
export type StateDiffFieldSelection = Selector<Exclude<keyof StateDiffFields, StateDiffRequiredFields>>
export type FieldSelection = {
    block?: BlockFieldSelection
    transaction?: TransactionFieldSelection
    receipt?: TransactionReceiptFieldSelection
    log?: LogFieldSelection
    trace?: TraceFieldSelection
    stateDiff?: StateDiffFieldSelection
}

type Trues<T> = Simplify<{
    [K in keyof T]-?: {[k: string]: boolean} extends T[K] ? Trues<T[K]> : true
}>

export type FieldSelectionAll = Trues<FieldSelection>

export type Block<F extends BlockFieldSelection = Trues<BlockFieldSelection>> = Simplify<
    Pick<BlockFields, BlockRequiredFields> & Select<BlockFields, F>
>

export type Transaction<F extends TransactionFieldSelection = Trues<TransactionFieldSelection>> = Simplify<
    Pick<TransactionFields, TransactionRequiredFields> & Select<TransactionFields, F>
>

export type TransactionReceipt<F extends TransactionReceiptFieldSelection = Trues<TransactionReceiptFieldSelection>> =
    Simplify<Pick<TransactionReceiptFields, TransactionReceiptRequiredFields> & Select<TransactionReceiptFields, F>>

export type Log<F extends LogFieldSelection = Trues<LogFieldSelection>> = Simplify<
    Pick<LogFields, LogRequiredFields> & Select<LogFields, F>
>

export type TraceCreateAction<F extends TraceCreateActionFieldSelection = Trues<TraceCreateActionFieldSelection>> =
    Simplify<Select<TraceCreateActionFields, F>>

export type TraceCreateResult<F extends TraceCreateResultFieldSelection = Trues<TraceCreateResultFieldSelection>> =
    Simplify<Select<TraceCreateResultFields, F>>

export type TraceCallAction<F extends TraceCallActionFieldSelection = Trues<TraceCallActionFieldSelection>> = Simplify<
    Select<TraceCallActionFields, F>
>

export type TraceCallResult<F extends TraceCallResultFieldSelection = Trues<TraceCallResultFieldSelection>> = Simplify<
    Select<TraceCallResultFields, F>
>

export type TraceSuicideAction<F extends TraceSuicideActionFieldSelection = Trues<TraceSuicideActionFieldSelection>> =
    Simplify<Select<TraceSuicideActionFields, F>>

export type TraceRewardAction<F extends TraceRewardActionFieldSelection = Trues<TraceRewardActionFieldSelection>> =
    Simplify<Select<TraceRewardActionFields, F>>

export type TraceBase = Pick<TraceBaseFields, Exclude<TraceRequiredFields, 'type'>>

type RemoveEmptyObjects<T> = {
    [K in keyof T as {} extends T[K] ? never : K]: T[K]
}

type Get<F, P extends keyof F> = Exclude<F[P], undefined>

export type TraceCreate<F extends TraceCreateFieldSelection = Trues<TraceCreateFieldSelection>> = Simplify<
    TraceBase & {type: 'create'} & Select<TraceCreateFields, F> &
        RemoveEmptyObjects<{action: TraceCreateAction<Get<F, 'action'>>; result?: TraceCreateResult<Get<F, 'result'>>}>
>

export type TraceCall<F extends TraceCallFieldSelection = Trues<TraceCallFieldSelection>> = Simplify<
    TraceBase & {type: 'call'} & Select<TraceCallFields, F> &
        RemoveEmptyObjects<{action: TraceCallAction<Get<F, 'action'>>; result?: TraceCallResult<Get<F, 'result'>>}>
>

export type TraceSuicide<F extends TraceSuicideFieldSelection = Trues<TraceSuicideFieldSelection>> = Simplify<
    TraceBase & {type: 'suicide'} & Select<TraceSuicideFields, F> &
        RemoveEmptyObjects<{action: TraceSuicideAction<Get<F, 'action'>>}>
>

export type TraceReward<F extends TraceRewardFieldSelection = Trues<TraceRewardFieldSelection>> = Simplify<
    TraceBase & {type: 'reward'} & Select<TraceRewardFields, F> &
        RemoveEmptyObjects<{action: TraceRewardAction<Get<F, 'action'>>}>
>

export type Trace<F extends TraceFieldSelection = Trues<TraceFieldSelection>> =
    | TraceCreate<MergeDefault<Get<F, 'create'>, F>>
    | TraceCall<MergeDefault<Get<F, 'call'>, F>>
    | TraceSuicide<MergeDefault<Get<F, 'suicide'>, F>>
    | TraceReward<MergeDefault<Get<F, 'reward'>, F>>

export type StateDiffBase = Pick<StateDiffBaseFields, StateDiffRequiredFields>

export type StateDiff<F extends StateDiffFieldSelection = Trues<StateDiffFieldSelection>> = Simplify<
    StateDiffBase & Select<StateDiffFields, F>
>

export type BlockData<F extends FieldSelection = {}> = {
    block: Block<Get<F, 'block'>>
    transactions: Transaction<Get<F, 'transaction'>>[]
    receipts: TransactionReceipt<Get<F, 'receipt'>>[]
    logs: Log<Get<F, 'log'>>[]
    traces: Trace<Get<F, 'trace'>>[]
    stateDiffs: StateDiff<Get<F, 'stateDiff'>>[]
}
