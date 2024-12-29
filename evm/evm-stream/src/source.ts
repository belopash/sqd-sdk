import {applyRangeBound, Range} from '@subsquid/util-internal-range'
import {PortalClient, PortalStreamData} from '@subsquid/portal-client'
import {EvmQuery} from './interfaces/data-request'
import {BlockData, FieldSelection} from './interfaces/data'
import {assertNotNull, AsyncQueue, unexpectedCase, weakMemo} from '@subsquid/util-internal'
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
    BlockHeader,
    Transaction,
    Log,
    TraceCreate,
    TraceCall,
    TraceSuicide,
    TraceReward,
    StateDiffNoChange,
    StateDiffAdd,
    StateDiffChange,
    StateDiffDelete,
    Block,
    StateDiff,
    Trace,
} from './mapping/entities'
import {setUpRelations} from './mapping/relations'
import {
    getBlockHeaderProps,
    getTxProps,
    getTxReceiptProps,
    getLogProps,
    getTraceFrameValidator,
    project,
} from './mapping/schema'

export interface HashAndHeight {
    hash: string
    height: number
}

export interface StreamData<Block> {
    finalizedHead: HashAndHeight
    rolledbackHeads: HashAndHeight[]
    blocks: Block[]
}

export interface DataSource<Block> {
    getHeight(): Promise<number>
    getFinalizedHeight(): Promise<number>
    getBlockStream(range?: Range): ReadableStream<Block>
}

export type GetDataSourceBlock<T> = T extends DataSource<infer B> ? B : never

export interface EvmPortalDataSourceOptions<Fields extends FieldSelection> {
    portal: string | PortalClient
    query: EvmQuery
    fields: Fields
}

export class EvmPortalDataSource<Fields extends FieldSelection, Block extends BlockData<Fields> = BlockData<Fields>>
    implements DataSource<StreamData<Block>>
{
    private portal: PortalClient
    private query: EvmQuery
    private fields: Fields

    constructor(options: EvmPortalDataSourceOptions<Fields>) {
        this.portal = typeof options.portal === 'string' ? new PortalClient({url: options.portal}) : options.portal
        this.query = options.query
        this.fields = options.fields
    }

    getHeight(): Promise<number> {
        return this.portal.getFinalizedHeight()
    }

    getFinalizedHeight(): Promise<number> {
        return this.portal.getFinalizedHeight()
    }

    getBlockStream(range?: Range): ReadableStream<StreamData<Block>> {
        let queue = new AsyncQueue<StreamData<Block>>(1)
        let ac = new AbortController()

        const ingest = async () => {
            let query = applyRangeBound(this.query.ranges, range)

            function abort() {
                reader?.cancel()
            }

            ac.signal.addEventListener('abort', abort)

            let reader: ReadableStreamDefaultReader<PortalStreamData<any>> | undefined
            try {
                for (let queryRange of query) {
                    let request = {
                        type: 'evm',
                        fromBlock: queryRange.range.from,
                        toBlock: queryRange.range.to,
                        fields: this.fields,
                        ...queryRange.request,
                    }

                    reader = this.portal.getFinalizedStream(request).getReader()

                    while (true) {
                        let data = await reader.read()
                        if (data.done) break

                        let blocks = data.value.blocks.map((b) => mapBlock(b, this.fields) as unknown as Block)

                        await queue.put({
                            finalizedHead: data.value.finalizedHead,
                            blocks,
                            rolledbackHeads: [],
                        })
                    }
                }
            } finally {
                ac.signal.removeEventListener('abort', abort)
                reader?.cancel()
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

export const getBlockValidator = weakMemo((fields: FieldSelection) => {
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

export function mapBlock(rawBlock: unknown, fields: FieldSelection): Block {
    let validator = getBlockValidator(fields)

    let src = cast(validator, rawBlock)

    let {number, hash, parentHash, ...hdr} = src.header
    if (hdr.timestamp) {
        hdr.timestamp = hdr.timestamp * 1000 // convert to ms
    }

    let header = new BlockHeader(number, hash, parentHash)
    Object.assign(header, hdr)

    let block = new Block(header)

    if (src.transactions) {
        for (let {transactionIndex, ...props} of src.transactions) {
            let tx = new Transaction(header, transactionIndex)
            Object.assign(tx, props)
            block.transactions.push(tx)
        }
    }

    if (src.logs) {
        for (let {logIndex, transactionIndex, ...props} of src.logs) {
            let log = new Log(header, logIndex, transactionIndex)
            Object.assign(log, props)
            block.logs.push(log)
        }
    }

    if (src.traces) {
        for (let {transactionIndex, traceAddress, type, ...props} of src.traces) {
            transactionIndex = assertNotNull(transactionIndex)
            let trace: Trace
            switch (type) {
                case 'create':
                    trace = new TraceCreate(header, transactionIndex, traceAddress)
                    break
                case 'call':
                    trace = new TraceCall(header, transactionIndex, traceAddress)
                    break
                case 'suicide':
                    trace = new TraceSuicide(header, transactionIndex, traceAddress)
                    break
                case 'reward':
                    trace = new TraceReward(header, transactionIndex, traceAddress)
                    break
                default:
                    throw unexpectedCase()
            }
            Object.assign(trace, props)
            block.traces.push(trace)
        }
    }

    if (src.stateDiffs) {
        for (let {transactionIndex, address, key, kind, ...props} of src.stateDiffs) {
            let diff: StateDiff
            switch (kind) {
                case '=':
                    diff = new StateDiffNoChange(header, transactionIndex, address, key)
                    break
                case '+':
                    diff = new StateDiffAdd(header, transactionIndex, address, key)
                    break
                case '*':
                    diff = new StateDiffChange(header, transactionIndex, address, key)
                    break
                case '-':
                    diff = new StateDiffDelete(header, transactionIndex, address, key)
                    break
                default:
                    throw unexpectedCase()
            }
            Object.assign(diff, props)
            block.stateDiffs.push(diff)
        }
    }

    setUpRelations(block)

    return block
}
