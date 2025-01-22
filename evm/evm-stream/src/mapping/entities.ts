import assert from 'assert'
import {
    BlockFields,
    LogFields,
    StateDiffAddFields,
    StateDiffBaseFields,
    StateDiffChangeFields,
    StateDiffDeleteFields,
    StateDiffNoChangeFields,
    TraceBaseFields,
    TraceCallFields,
    TraceCreateFields,
    TraceRewardFields,
    TraceSuicideFields,
    TransactionFields,
    TransactionReceiptFields,
} from '../interfaces/evm'
import type {Block, Log, Transaction, TransactionReceipt, TraceBase, StateDiffBase} from '../interfaces/data'

export interface BlockEntity extends BlockFields {}
export class BlockEntity implements Block {
    #transactions?: TransactionEntity[]
    #receipts?: TransactionReceiptEntity[]
    #logs?: LogEntity[]
    #traces?: TraceEntity[]
    #stateDiffs?: StateDiffEntity[]

    get transactions(): TransactionEntity[] {
        return this.#transactions || (this.#transactions = [])
    }

    set transactions(value: TransactionEntity[]) {
        this.#transactions = value
    }

    get receipts(): TransactionReceiptEntity[] {
        return this.#receipts || (this.#receipts = [])
    }

    set receipts(value: TransactionReceiptEntity[]) {
        this.#receipts = value
    }

    get logs(): LogEntity[] {
        return this.#logs || (this.#logs = [])
    }

    set logs(value: LogEntity[]) {
        this.#logs = value
    }

    get traces(): TraceEntity[] {
        return this.#traces || (this.#traces = [])
    }

    set traces(value: TraceEntity[]) {
        this.#traces = value
    }

    get stateDiffs(): StateDiffEntity[] {
        return this.#stateDiffs || (this.#stateDiffs = [])
    }

    set stateDiffs(value: StateDiffEntity[]) {
        this.#stateDiffs = value
    }
}

export interface TransactionEntity extends TransactionFields {}
export class TransactionEntity implements Transaction {
    #block?: BlockEntity
    #receipt?: TransactionReceiptEntity
    #logs?: LogEntity[]
    #traces?: TraceEntity[]
    #stateDiffs?: StateDiffEntity[]

    constructor() {}

    get block(): BlockEntity {
        assert(this.#block != null)
        return this.#block
    }

    set block(value: BlockEntity) {
        this.#block = value
    }

    get receipt(): TransactionReceiptEntity | undefined {
        return this.#receipt
    }

    set receipt(value: TransactionReceiptEntity | undefined) {
        this.#receipt = value
    }

    getReceipt(): TransactionReceiptEntity {
        assert(this.receipt != null)
        return this.receipt
    }

    get logs(): LogEntity[] {
        return this.#logs || (this.#logs = [])
    }

    set logs(value: LogEntity[]) {
        this.#logs = value
    }

    get traces(): TraceEntity[] {
        return this.#traces || (this.#traces = [])
    }

    set traces(value: TraceEntity[]) {
        this.#traces = value
    }

    get stateDiffs(): StateDiffEntity[] {
        return this.#stateDiffs || (this.#stateDiffs = [])
    }

    set stateDiffs(value: StateDiffEntity[]) {
        this.#stateDiffs = value
    }
}

export interface TransactionReceiptEntity extends TransactionReceiptFields {}
export class TransactionReceiptEntity implements TransactionReceipt {
    #block?: BlockEntity
    #transaction?: TransactionEntity
    #logs?: LogEntity[]
    #traces?: TraceEntity[]
    #stateDiffs?: StateDiffEntity[]

    get block(): BlockEntity {
        assert(this.#block != null)
        return this.#block
    }

    set block(value: BlockEntity) {
        this.#block = value
    }

    get transaction(): TransactionEntity | undefined {
        return this.#transaction
    }

    set transaction(value: TransactionEntity | undefined) {
        this.#transaction = value
    }

    getTransaction(): TransactionEntity {
        assert(this.transaction != null)
        return this.transaction
    }

    get logs(): LogEntity[] {
        return this.#logs || (this.#logs = [])
    }

    set logs(value: LogEntity[]) {
        this.#logs = value
    }

    get traces(): TraceEntity[] {
        return this.#traces || (this.#traces = [])
    }

    set traces(value: TraceEntity[]) {
        this.#traces = value
    }

    get stateDiffs(): StateDiffEntity[] {
        return this.#stateDiffs || (this.#stateDiffs = [])
    }

    set stateDiffs(value: StateDiffEntity[]) {
        this.#stateDiffs = value
    }
}

export interface LogEntity extends LogFields {}
export class LogEntity implements Log {
    #block?: BlockEntity
    #transaction?: TransactionEntity
    #receipt?: TransactionReceiptEntity

    get block(): BlockEntity {
        assert(this.#block != null)
        return this.#block
    }

    set block(value: BlockEntity) {
        this.#block = value
    }

    get transaction(): TransactionEntity | undefined {
        return this.#transaction
    }

    set transaction(value: TransactionEntity | undefined) {
        this.#transaction = value
    }

    getTransaction(): TransactionEntity {
        assert(this.transaction != null)
        return this.transaction
    }

    get receipt(): TransactionReceiptEntity | undefined {
        return this.#receipt
    }

    set receipt(value: TransactionReceiptEntity | undefined) {
        this.#receipt = value
    }

    getReceipt(): TransactionReceiptEntity {
        assert(this.receipt != null)
        return this.receipt
    }
}

interface TraceBaseEntity extends TraceBaseFields {}
class TraceBaseEntity implements TraceBase {
    #block?: BlockEntity
    #transaction?: TransactionEntity
    #receipt?: TransactionReceiptEntity
    #parent?: TraceEntity
    #children?: TraceEntity[]

    get block(): BlockEntity {
        assert(this.#block != null)
        return this.#block
    }

    set block(value: BlockEntity) {
        this.#block = value
    }

    get transaction(): TransactionEntity | undefined {
        return this.#transaction
    }

    set transaction(value: TransactionEntity | undefined) {
        this.#transaction = value
    }

    getTransaction(): TransactionEntity {
        assert(this.transaction != null)
        return this.transaction
    }

    get receipt(): TransactionReceiptEntity | undefined {
        return this.#receipt
    }

    set receipt(value: TransactionReceiptEntity | undefined) {
        this.#receipt = value
    }

    getReceipt(): TransactionReceiptEntity {
        assert(this.receipt != null)
        return this.receipt
    }

    get parent(): TraceEntity | undefined {
        return this.#parent
    }

    set parent(value: TraceEntity | undefined) {
        this.#parent = value
    }

    getParent(): TraceEntity {
        assert(this.parent != null)
        return this.parent
    }

    get children(): TraceEntity[] {
        return this.#children || (this.#children = [])
    }

    set children(value: TraceEntity[]) {
        this.#children = value
    }
}

export interface TraceCreateEntity extends TraceCreateFields {}
export class TraceCreateEntity extends TraceBaseEntity {}

export interface TraceCallEntity extends TraceCallFields {}
export class TraceCallEntity extends TraceBaseEntity {}

export interface TraceSuicideEntity extends TraceSuicideFields {}
export class TraceSuicideEntity extends TraceBaseEntity {}

export interface TraceRewardEntity extends TraceRewardFields {}
export class TraceRewardEntity extends TraceBaseEntity {}

export type TraceEntity = TraceCreateEntity | TraceCallEntity | TraceSuicideEntity | TraceRewardEntity

interface StateDiffBaseEntity extends StateDiffBaseFields {}
class StateDiffBaseEntity implements StateDiffBase {
    #block?: BlockEntity
    #transaction?: TransactionEntity

    get block(): BlockEntity {
        assert(this.#block != null)
        return this.#block
    }

    set block(value: BlockEntity) {
        this.#block = value
    }

    get transaction(): TransactionEntity | undefined {
        return this.#transaction
    }

    set transaction(value: TransactionEntity | undefined) {
        this.#transaction = value
    }

    getTransaction(): TransactionEntity {
        assert(this.transaction != null)
        return this.transaction
    }
}

export interface StateDiffNoChangeEntity extends StateDiffNoChangeFields {}
export class StateDiffNoChangeEntity extends StateDiffBaseEntity {
    kind: '=' = '='
}

export interface StateDiffAddEntity extends StateDiffAddFields {}
export class StateDiffAddEntity extends StateDiffBaseEntity {
    kind: '+' = '+'
}

export interface StateDiffChangeEntity extends StateDiffChangeFields {}
export class StateDiffChangeEntity extends StateDiffBaseEntity {
    kind: '*' = '*'
}

export interface StateDiffDeleteEntity extends StateDiffDeleteFields {}
export class StateDiffDeleteEntity extends StateDiffBaseEntity {
    kind: '-' = '-'
}

export type StateDiffEntity =
    | StateDiffNoChangeEntity
    | StateDiffAddEntity
    | StateDiffChangeEntity
    | StateDiffDeleteEntity
