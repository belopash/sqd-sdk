import {applyRangeBound, getRequestAt, Range} from '@subsquid/util-internal-range'
import {PortalClient, PortalStreamData} from '@subsquid/portal-client'
import {weakMemo} from '@subsquid/util-internal'
import {array, BYTES, cast, NAT, object, STRING, taggedUnion, withDefault} from '@subsquid/util-internal-validation'
import {
    getBlockHeaderProps,
    getTxProps,
    getTxReceiptProps,
    getLogProps,
    getTraceFrameValidator,
    project,
} from './mapping/schema'
import {BlockData, EvmQueryOptions, FieldSelection, Response} from './query'
import {Bytes, Simplify} from '@subsquid/util-types'

export interface HashAndNumber {
    hash: Bytes
    number: number
}

export type DataSourceStreamData<B> = {
    finalizedHead?: HashAndNumber
    blocks: (B & {[DataSource.blockRef]: HashAndNumber})[]
}

export type DataSourceStream<B> = ReadableStream<DataSourceStreamData<B>>

export class DataSource<B> {
    static readonly blockRef = Symbol('block ref')
}

export interface DataSource<B> {
    getHeight(): Promise<number>
    getFinalizedHeight(): Promise<number>
    getBlockStream(range?: Range): DataSourceStream<B>
}

export type GetDataSourceBlock<T> = T extends DataSource<infer B> ? B : never

export interface EvmPortalDataSourceOptions<F extends FieldSelection> {
    portal: string | PortalClient
    query: EvmQueryOptions<F>
}

export class EvmPortalDataSource<F extends FieldSelection, B extends BlockData<F> = BlockData<F>>
    implements DataSource<B>
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

    getBlockStream(range?: Range, stopOnHead?: boolean): DataSourceStream<B> {
        let requests = applyRangeBound(this.query.requests, range)
        let fields = getFields(this.query.fields)

        let {writable, readable} = new TransformStream<PortalStreamData<Response<any>>, DataSourceStreamData<B>>({
            transform: (data, controller) => {
                controller.enqueue({
                    finalizedHead: data.finalizedHead,
                    blocks: data.blocks.map((b) => {
                        let block = mapBlock(b, fields) as unknown as B & {[DataSource.blockRef]: HashAndNumber}
                        block[DataSource.blockRef] = {hash: b.header.hash, number: b.header.number}
                        return block
                    }),
                })
            },
        })

        const ingest = async () => {
            for (let request of requests) {
                let query = {
                    type: 'evm',
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                }

                await this.portal.getFinalizedStream(query, {stopOnHead}).pipeTo(writable, {
                    preventClose: true,
                })
            }
        }

        ingest()
            .then(
                () => writable.close(),
                (reason) => writable.abort(reason)
            )
            .catch(() => {})

        return readable
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
        transactions: withDefault([], array(Transaction)),
        logs: withDefault([], array(Log)),
        traces: withDefault([], array(Trace)),
        stateDiffs: withDefault([], array(StateDiff)),
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
