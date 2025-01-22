import {applyRangeBound, Range} from '@subsquid/util-internal-range'
import {PortalClient, PortalStreamData} from '@subsquid/portal-client'
import {AsyncQueue, weakMemo} from '@subsquid/util-internal'
import {
    array,
    BYTES,
    cast,
    NAT,
    object,
    option,
    STRING,
    taggedUnion,
    withDefault,
} from '@subsquid/util-internal-validation'
import {
    getBlockHeaderProps,
    getTxProps,
    getTxReceiptProps,
    getLogProps,
    getTraceFrameValidator,
    project,
} from './mapping/schema'
import {BlockData, FieldSelection} from '@subsquid/portal-client/lib/query/evm'
import {EvmQueryOptions} from './builder'

export interface HashAndNumber {
    hash: string
    number: number
}

export interface StreamData<B> {
    finalizedHead: HashAndNumber
    rollbackHeads: HashAndNumber[]
    blocks: B[]
}

export interface DataSource<B> {
    getHeight(): Promise<number>
    getFinalizedHeight(): Promise<number>
    getBlockStream(range?: Range): ReadableStream<B>
}

export type GetDataSourceBlock<T> = T extends DataSource<infer B> ? B : never

export interface EvmPortalDataSourceOptions<F extends FieldSelection> {
    portal: string | PortalClient
    query: EvmQueryOptions<F>
}

export class EvmPortalDataSource<F extends FieldSelection, B extends BlockData<F> = BlockData<F>>
    implements DataSource<StreamData<B>>
{
    private portal: PortalClient
    private query: EvmQueryOptions<F>

    constructor(options: EvmPortalDataSourceOptions<F>) {
        this.portal = typeof options.portal === 'string' ? new PortalClient({url: options.portal}) : options.portal
        this.query = options.query
    }

    getHeight(): Promise<number> {
        return this.portal.getFinalizedHeight()
    }

    getFinalizedHeight(): Promise<number> {
        return this.portal.getFinalizedHeight()
    }

    getBlockStream(range?: Range, stopOnHead?: boolean): ReadableStream<StreamData<B>> {
        let queue = new AsyncQueue<StreamData<B>>(1)
        let ac = new AbortController()

        const ingest = async () => {
            let requests = applyRangeBound(this.query.requests, range)
            let fields = getFields(this.query.fields)

            function abort() {
                reader?.cancel()
            }

            ac.signal.addEventListener('abort', abort)

            let reader: ReadableStreamDefaultReader<PortalStreamData<any>> | undefined
            try {
                for (let request of requests) {
                    let query = {
                        type: 'evm',
                        fromBlock: request.range.from,
                        toBlock: request.range.to,
                        fields,
                        ...request.request,
                    }

                    reader = this.portal.getFinalizedStream(query, {stopOnHead}).getReader()

                    while (true) {
                        let data = await reader.read()
                        if (data.done) break

                        let blocks = data.value.blocks.map((b) => mapBlock(b, fields) as unknown as B)

                        await queue.put({
                            finalizedHead: data.value.finalizedHead,
                            blocks,
                            rollbackHeads: [],
                        })
                    }
                }
            } finally {
                reader?.cancel().catch(() => {})
            }
        }

        return new ReadableStream({
            start(controller) {
                ingest()
                    .then(() => {
                        queue.close()
                    })
                    .catch((error) => {
                        if (queue.isClosed()) return
                        queue.close()
                        controller.error(error)
                    })
            },
            async pull(controller) {
                let value = await queue.take()
                if (value) {
                    controller.enqueue(value)
                } else {
                    controller.close()
                }
            },
            cancel() {
                ac.abort()
            },
        })
    }
}

export const getBlockValidator = weakMemo(<F extends FieldSelection>(fields: F) => {
    let BlockHeader = object(getBlockHeaderProps(fields.block, true))

    let Transaction = object({
        hash: fields.transaction?.hash ? BYTES : undefined,
        ...getTxProps(fields.transaction, true),
        sighash: fields.transaction?.sighash ? withDefault('0x', BYTES) : undefined,
        ...getTxReceiptProps(fields.transaction, true),
    })

    let Log = object(getLogProps(fields.log, true))

    let Trace = getTraceFrameValidator(fields.trace, true)

    let stateDiffBase = {
        transactionIndex: NAT,
        address: BYTES,
        key: STRING,
    }

    let StateDiff = taggedUnion('kind', {
        ['=']: object({...stateDiffBase}),
        ['+']: object({...stateDiffBase, ...project(fields.stateDiff, {next: BYTES})}),
        ['*']: object({...stateDiffBase, ...project(fields.stateDiff, {prev: BYTES, next: BYTES})}),
        ['-']: object({...stateDiffBase, ...project(fields.stateDiff, {prev: BYTES})}),
    })

    return object({
        header: BlockHeader,
        transactions: option(array(Transaction)),
        logs: option(array(Log)),
        traces: option(array(Trace)),
        stateDiffs: option(array(StateDiff)),
    })
})

export function mapBlock<F extends FieldSelection, B extends BlockData<F> = BlockData<F>>(
    rawBlock: unknown,
    fields: F
): B {
    let validator = getBlockValidator(fields)

    let block = cast(validator, rawBlock)

    // let {number, hash, parentHash, ...hdr} = src.header
    // if (hdr.timestamp) {
    //     hdr.timestamp = hdr.timestamp * 1000 // convert to ms
    // }

    // let header = new BlockHeader(number, hash, parentHash)
    // Object.assign(header, hdr)

    // let block = new Block(header)

    // if (src.transactions) {
    //     for (let {transactionIndex, ...props} of src.transactions) {
    //         let tx = new Transaction(header, transactionIndex)
    //         Object.assign(tx, props)
    //         block.transactions.push(tx)
    //     }
    // }

    // if (src.logs) {
    //     for (let {logIndex, transactionIndex, ...props} of src.logs) {
    //         let log = new Log(header, logIndex, transactionIndex)
    //         Object.assign(log, props)
    //         block.logs.push(log)
    //     }
    // }

    // if (src.traces) {
    //     for (let {transactionIndex, traceAddress, type, ...props} of src.traces) {
    //         transactionIndex = assertNotNull(transactionIndex)
    //         let trace: Trace
    //         switch (type) {
    //             case 'create':
    //                 trace = new TraceCreate(header, transactionIndex, traceAddress)
    //                 break
    //             case 'call':
    //                 trace = new TraceCall(header, transactionIndex, traceAddress)
    //                 break
    //             case 'suicide':
    //                 trace = new TraceSuicide(header, transactionIndex, traceAddress)
    //                 break
    //             case 'reward':
    //                 trace = new TraceReward(header, transactionIndex, traceAddress)
    //                 break
    //             default:
    //                 throw unexpectedCase()
    //         }
    //         Object.assign(trace, props)
    //         block.traces.push(trace)
    //     }
    // }

    // if (src.stateDiffs) {
    //     for (let {transactionIndex, address, key, kind, ...props} of src.stateDiffs) {
    //         let diff: StateDiff
    //         switch (kind) {
    //             case '=':
    //                 diff = new StateDiffNoChange(header, transactionIndex, address, key)
    //                 break
    //             case '+':
    //                 diff = new StateDiffAdd(header, transactionIndex, address, key)
    //                 break
    //             case '*':
    //                 diff = new StateDiffChange(header, transactionIndex, address, key)
    //                 break
    //             case '-':
    //                 diff = new StateDiffDelete(header, transactionIndex, address, key)
    //                 break
    //             default:
    //                 throw unexpectedCase()
    //         }
    //         Object.assign(diff, props)
    //         block.stateDiffs.push(diff)
    //     }
    // }

    // setUpRelations(block)

    return block as unknown as B
}

function getFields(fields?: FieldSelection): FieldSelection {
    return {
        block: {...fields?.block, ...ALWAYS_SELECTED_FIELDS.block},
        transaction: {...fields?.transaction, ...ALWAYS_SELECTED_FIELDS.transaction},
        log: {...fields?.log, ...ALWAYS_SELECTED_FIELDS.log},
        trace: {...fields?.trace, ...ALWAYS_SELECTED_FIELDS.trace},
        stateDiff: {...fields?.stateDiff, ...ALWAYS_SELECTED_FIELDS.stateDiff, kind: true},
    }
}

const ALWAYS_SELECTED_FIELDS = {
    block: {
        number: true,
        hash: true,
        parentHash: true,
    },
    transaction: {
        transactionIndex: true,
    },
    log: {
        logIndex: true,
        transactionIndex: true,
    },
    trace: {
        transactionIndex: true,
        traceAddress: true,
        type: true,
    },
    stateDiff: {
        transactionIndex: true,
        address: true,
        key: true,
    },
} as const
