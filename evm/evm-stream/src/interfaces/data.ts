import {Get, OverrideKeys, Select, Selector, Simplify} from '@subsquid/util-types'
import {
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

export type Block<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Pick<BlockFields, BlockRequiredFields> & Select<BlockFields, Get<F, 'block'>>
>

export type Transaction<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Pick<TransactionFields, TransactionRequiredFields> & Select<TransactionFields, Get<F, 'transaction'>>
>

export type TransactionReceipt<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Pick<TransactionReceiptFields, TransactionReceiptRequiredFields> &
        Select<TransactionReceiptFields, Get<F, 'receipt'>>
>

export type Log<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Pick<LogFields, LogRequiredFields> & Select<LogFields, Get<F, 'log'>>
>

export type TraceCreateAction<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Select<TraceCreateActionFields, Get<F, 'trace.create.action'>>
>

export type TraceCreateResult<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Select<TraceCreateResultFields, Get<F, 'trace.create.result'>>
>

export type TraceCallAction<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Select<TraceCallActionFields, Get<F, 'trace.call.action'>>
>

export type TraceCallResult<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Select<TraceCallResultFields, Get<F, 'trace.call.result'>>
>

export type TraceSuicideAction<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Select<TraceSuicideActionFields, Get<F, 'trace.suicide.action'>>
>

export type TraceRewardAction<F extends FieldSelection = FieldSelectionAll> = Simplify<
    Select<TraceRewardActionFields, Get<F, 'trace.reward.action'>>
>

export type TraceBase<F extends FieldSelection = FieldSelectionAll> = Pick<
    TraceBaseFields,
    Exclude<TraceRequiredFields, 'type'>
>

type RemoveEmptyObjects<T> = {
    [K in keyof T as {} extends T[K] ? never : K]: T[K]
}

export type TraceCreate<F extends FieldSelection = FieldSelectionAll> = Simplify<
    TraceBase<F> & {type: 'create'} & Select<TraceCreateFields, OverrideKeys<Get<F, 'trace'>, Get<F, 'trace.create'>>> &
        RemoveEmptyObjects<{action: TraceCreateAction<F>; result?: TraceCreateResult<F>}>
>

export type TraceCall<F extends FieldSelection = FieldSelectionAll> = Simplify<
    TraceBase<F> & {type: 'call'} & Select<TraceCallFields, OverrideKeys<Get<F, 'trace'>, Get<F, 'trace.call'>>> &
        RemoveEmptyObjects<{action: TraceCallAction<F>; result?: TraceCallResult<F>}>
>

export type TraceSuicide<F extends FieldSelection = FieldSelectionAll> = Simplify<
    TraceBase<F> & {type: 'suicide'} & Select<
            TraceSuicideFields,
            OverrideKeys<Get<F, 'trace'>, Get<F, 'trace.suicide'>>
        > &
        RemoveEmptyObjects<{action: TraceSuicideAction<F>}>
>

export type TraceReward<F extends FieldSelection = FieldSelectionAll> = Simplify<
    TraceBase<F> & {type: 'reward'} & Select<TraceRewardFields, OverrideKeys<Get<F, 'trace'>, Get<F, 'trace.reward'>>> &
        RemoveEmptyObjects<{action: TraceRewardAction<F>}>
>

export type Trace<F extends FieldSelection = FieldSelectionAll> =
    | TraceCreate<F>
    | TraceCall<F>
    | TraceSuicide<F>
    | TraceReward<F>

export type StateDiffBase<F extends FieldSelection = FieldSelectionAll> = Pick<
    StateDiffBaseFields,
    StateDiffRequiredFields
>

export type StateDiff<F extends FieldSelection = FieldSelectionAll> = Simplify<
    StateDiffBase<F> & Select<StateDiffFields, Get<F, 'stateDiff'>>
>
