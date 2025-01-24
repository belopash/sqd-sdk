import {maybeLast, unexpectedCase} from '@subsquid/util-internal'
import {
    BlockEntity,
    LogEntity,
    StateDiffAddEntity,
    StateDiffChangeEntity,
    StateDiffDeleteEntity,
    StateDiffNoChangeEntity,
    TraceCallEntity,
    TraceCreateEntity,
    TraceEntity,
    TraceRewardEntity,
    TraceSuicideEntity,
    TransactionEntity,
    TransactionReceiptEntity,
} from './entities'
import {
    BlockFields,
    LogFields,
    StateDiffFields,
    TraceFields,
    TransactionFields,
    TransactionReceiptFields,
} from '../interfaces/evm'
import {
    Block,
    BlockRequiredFields,
    FieldSelection,
    LogRequiredFields,
    StateDiffRequiredFields,
    TraceRequiredFields,
    TransactionReceiptRequiredFields,
    TransactionRequiredFields,
} from '../interfaces/data'

export function setUpRelations<F extends FieldSelection>(data: {
    block: Pick<BlockFields, BlockRequiredFields>
    logs: Pick<LogFields, LogRequiredFields>[]
    transactions: Pick<TransactionFields, TransactionRequiredFields>[]
    receipts: Pick<TransactionReceiptFields, TransactionReceiptRequiredFields>[]
    traces: Pick<TraceFields, TraceRequiredFields>[]
    stateDiffs: Pick<StateDiffFields, StateDiffRequiredFields>[]
}): Block<any> {
    let block = Object.assign(new BlockEntity(), data.block)

    block.logs = data.logs.map((log) => Object.assign(new LogEntity(), log)).sort((a, b) => a.logIndex - b.logIndex)
    block.transactions = data.transactions
        .map((tx) => Object.assign(new TransactionEntity(), tx))
        .sort((a, b) => a.transactionIndex - b.transactionIndex)
    block.receipts = data.receipts
        .map((rec) => Object.assign(new TransactionReceiptEntity(), rec))
        .sort((a, b) => a.transactionIndex - b.transactionIndex)
    block.traces = data.traces
        .map((trace) => {
            switch (trace.type) {
                case 'create':
                    return Object.assign(new TraceCreateEntity(), trace)
                case 'call':
                    return Object.assign(new TraceCallEntity(), trace)
                case 'reward':
                    return Object.assign(new TraceRewardEntity(), trace)
                case 'suicide':
                    return Object.assign(new TraceSuicideEntity(), trace)
                default:
                    throw unexpectedCase(trace.type)
            }
        })
        .sort(traceCompare)
    block.stateDiffs = data.stateDiffs.map((diff) => {
        switch (diff.kind) {
            case '=':
                return Object.assign(new StateDiffNoChangeEntity(), diff)
            case '+':
                return Object.assign(new StateDiffAddEntity(), diff)
            case '-':
                return Object.assign(new StateDiffDeleteEntity(), diff)
            case '*':
                return Object.assign(new StateDiffChangeEntity(), diff)
            default:
                throw unexpectedCase(diff.kind)
        }
    })

    let txs: (TransactionEntity | undefined)[] = new Array((maybeLast(block.transactions)?.transactionIndex ?? -1) + 1)
    for (let tx of block.transactions) {
        txs[tx.transactionIndex] = tx
    }

    for (let rec of block.logs) {
        let tx = txs[rec.transactionIndex]
        if (tx) {
            rec.transaction = tx
            tx.logs.push(rec)
        }
    }

    for (let i = 0; i < block.traces.length; i++) {
        let rec = block.traces[i]
        let tx = txs[rec.transactionIndex]
        if (tx) {
            rec.transaction = tx
            tx.traces.push(rec)
        }
        for (let j = i + 1; j < block.traces.length; j++) {
            let next = block.traces[j]
            if (isDescendent(rec, next)) {
                rec.children.push(next)
                if (next.traceAddress.length == rec.traceAddress.length + 1) {
                    next.parent = rec
                }
            } else {
                break
            }
        }
    }

    for (let rec of block.stateDiffs) {
        let tx = txs[rec.transactionIndex]
        if (tx) {
            rec.transaction = tx
            tx.stateDiffs.push(rec)
        }
    }

    return block as Block<any>
}

function traceCompare(a: TraceEntity, b: TraceEntity) {
    return a.transactionIndex - b.transactionIndex || addressCompare(a.traceAddress, b.traceAddress)
}

function addressCompare(a: number[], b: number[]): number {
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
        let order = a[i] - b[i]
        if (order) return order
    }
    return a.length - b.length // this differs from substrate call ordering
}

function isDescendent(parent: TraceEntity, child: TraceEntity): boolean {
    if (parent.transactionIndex != child.transactionIndex) return false
    if (parent.traceAddress.length >= child.traceAddress.length) return false
    for (let i = 0; i < parent.traceAddress.length; i++) {
        if (parent.traceAddress[i] != child.traceAddress[i]) return false
    }
    return true
}
