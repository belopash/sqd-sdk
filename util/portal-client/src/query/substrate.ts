import {Bytes, Bytes32, ExcludeUndefined, Select, Selector, Simplify} from '@subsquid/util-types'

export type QualifiedName = string & {}

export type BlockHeaderFields = {
    /**
     * Block height
     */
    number: number
    /**
     * Block hash
     */
    hash: Bytes32
    /**
     * Hash of the parent block
     */
    parentHash: Bytes32
    /**
     * Root hash of the state merkle tree
     */
    stateRoot: Bytes
    /**
     * Root hash of the extrinsics merkle tree
     */
    extrinsicsRoot: Bytes
    digest: {logs: Bytes[]}
    specName: string
    specVersion: number
    implName: string
    implVersion: number
    /**
     * Block timestamp as set by `timestamp.now()` (unix epoch ms, compatible with `Date`).
     */
    timestamp?: number
    /**
     * Account address of block validator
     */
    validator?: Bytes32
}

export type ExtrinsicSignatureFields = {
    address: unknown
    signature: unknown
    signedExtensions: unknown
}

export type ExtrinsicFields = {
    /**
     * Ordinal index in the extrinsics array of the current block
     */
    index: number
    version: number
    signature?: ExtrinsicSignatureFields
    fee?: bigint
    tip?: bigint
    error?: unknown
    success?: boolean
    /**
     * Blake2b 128-bit hash of the raw extrinsic
     */
    hash?: Bytes32
}

export type CallFields = {
    extrinsicIndex: number
    address: number[]
    name: QualifiedName
    args: unknown
    origin?: unknown
    /**
     * Call error.
     *
     * Absence of error doesn't imply that the call was executed successfully,
     * check {@link success} property for that.
     */
    error?: unknown
    success?: boolean
    _ethereumTransactTo?: Bytes
    _ethereumTransactSighash?: Bytes
}

export type EventFields = {
    /**
     * Ordinal index in the event array of the current block
     */
    index: number
    /**
     * Event name
     */
    name: QualifiedName
    args: unknown
    phase: 'Initialization' | 'ApplyExtrinsic' | 'Finalization'
    extrinsicIndex?: number
    callAddress?: number[]
    /**
     * This field is not supported by all currently deployed archives.
     * Requesting it may cause internal error.
     */
    topics: Bytes[]
    _evmLogAddress?: Bytes
    _evmLogTopics?: Bytes[]
    _contractAddress?: Bytes
    _gearProgramId?: Bytes
}

type Trues<T> = {[K in keyof T]-?: true}

export type BlockHeaderFieldSelection = Simplify<Selector<keyof BlockHeaderFields>>
export type BlockHeader<T extends BlockHeaderFieldSelection = Trues<BlockHeaderFieldSelection>> = Simplify<
    Select<BlockHeaderFields, T>
>

export type ExtrinsicFieldSelection = Simplify<Selector<keyof ExtrinsicFields>>
export type Extrinsic<T extends ExtrinsicFieldSelection = Trues<ExtrinsicFields>> = Simplify<Select<ExtrinsicFields, T>>

export type CallFieldSelection = Simplify<Selector<keyof CallFields>>
export type Call<T extends CallFieldSelection = Trues<CallFields>> = Simplify<Select<CallFields, T>>

export type EventFieldSelection = Simplify<Selector<keyof EventFields>>
export type Event<T extends EventFieldSelection = Trues<EventFields>> = Simplify<Select<EventFields, T>>

export type FieldSelection = {
    block: BlockHeaderFieldSelection
    extrinsic?: ExtrinsicFieldSelection
    call?: CallFieldSelection
    event?: EventFieldSelection
}

export type EventRelations = {
    extrinsic?: boolean
    call?: boolean
    stack?: boolean
}

export type EventRequest = Simplify<
    {
        name?: QualifiedName[]
    } & EventRelations
>

export type CallRelations = {
    extrinsic?: boolean
    stack?: boolean
    events?: boolean
}

export type CallRequest = Simplify<{name?: QualifiedName[]} & CallRelations>

export type EvmLogRequest = Simplify<{address?: Bytes[]} & EventRelations>

export type EthereumLogRequest = Simplify<
    {
        address?: Bytes[]
        topic0?: Bytes[]
        topic1?: Bytes[]
        topic2?: Bytes[]
        topic3?: Bytes[]
    } & EventRelations
>

export type EthereumTransactRequest = Simplify<{to?: Bytes[]; sighash?: Bytes[]} & CallRelations>

export type ContractsContractEmittedRequest = Simplify<{address?: Bytes[]} & EventRelations>

export type GearMessageQueuedRequest = Simplify<{programId?: Bytes[]} & EventRelations>

export type GearUserMessageSentRequest = Simplify<{programId?: Bytes[]} & EventRelations>

export type DataRequest = {
    includeAllBlocks?: boolean
    events?: EventRequest[]
    calls?: CallRequest[]
    evmLogs?: EvmLogRequest[]
    ethereumTransactions?: EthereumTransactRequest[]
    contractsEvents?: ContractsContractEmittedRequest[]
    gearMessagesQueued?: GearMessageQueuedRequest[]
    gearUserMessagesSent?: GearUserMessageSentRequest[]
}

export type FinalizedQuery = Simplify<
    {
        type: 'substrate'
        fromBlock?: number
        toBlock?: number
        fields: FieldSelection
    } & DataRequest
>

export type BlockData<F extends FieldSelection> = Simplify<{
    header: BlockHeader<F['block']>
    events?: Event<ExcludeUndefined<F['event']>>[]
    calls?: Call<ExcludeUndefined<F['call']>>[]
    extrinsics?: Extrinsic<ExcludeUndefined<F['extrinsic']>>[]
}>

export type Response<Q extends FinalizedQuery> = BlockData<Q['fields']>
