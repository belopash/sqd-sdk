import type {
    FieldSelection as EvmFieldSelection,
    RequestOptions,
    DataRequest as EvmDataRequest,
    Log,
    LogRequest,
} from '@subsquid/evm-stream'
import {EvmPortalDataSource} from '@subsquid/evm-stream'
import {PortalClient} from '@subsquid/portal-client'
import type {Bytes20, Select, Selector, Simplify} from '@subsquid/util-types'
import type {RangeRequest, Range} from '@subsquid/util-internal-range'
import {getRequestAt} from '@subsquid/util-internal-range'
import * as erc20 from './abi/erc20'

type Trues<T> = Simplify<{
    [K in keyof T]-?: {[k: string]: boolean} extends T[K] ? Trues<T[K]> : true
}>

export type TransferFields = {
    logIndex: number
    transactionIndex: number
    address: Bytes20
    from: Bytes20
    to: Bytes20
    value: bigint
}
export type TransferFieldSelection = Selector<keyof TransferFields>
export type Transfer<F extends TransferFieldSelection = Trues<TransferFieldSelection>> = Select<TransferFields, F>

export type Erc20FieldSelection = EvmFieldSelection & {
    transfer?: TransferFieldSelection
}

export type TransferRequest = Simplify<
    {
        from?: Bytes20[]
        to?: Bytes20[]
        log?: boolean
    } & Pick<LogRequest, 'address' | 'transaction' | 'transactionLogs' | 'transactionStateDiffs' | 'transactionTraces'>
>

export type TransferRequestOptions = RequestOptions<TransferRequest>

export type Erc20DataRequest = EvmDataRequest & {
    transfers?: TransferRequest[]
}

export type Erc20QueryOptions<F extends Erc20FieldSelection> = {
    fields: F
    requests: RangeRequest<Erc20DataRequest>[]
}

export interface Erc20DataSourceOptions<F extends Erc20FieldSelection> {
    portal: string | PortalClient
    query: Erc20QueryOptions<F>
}

export class ERC20DataSource<F extends Erc20FieldSelection> {
    private ds: EvmPortalDataSource<F>
    private query: Erc20QueryOptions<F>

    constructor(options: Erc20DataSourceOptions<F>) {
        this.ds = new EvmPortalDataSource({
            portal: options.portal,
            query: {
                fields: {
                    block: options.query.fields.block,
                    transaction: options.query.fields.transaction,
                    stateDiff: options.query.fields.stateDiff,
                    trace: options.query.fields.trace,
                    log: {
                        ...options.query.fields.log,
                        address: options.query.fields.log?.address || options.query.fields.transfer?.address,
                        data:
                            options.query.fields.log?.data ||
                            options.query.fields.transfer?.value ||
                            options.query.fields.transfer?.from ||
                            options.query.fields.transfer?.to,
                        topics:
                            options.query.fields.log?.topics ||
                            options.query.fields.transfer?.value ||
                            options.query.fields.transfer?.from ||
                            options.query.fields.transfer?.to,
                    },
                } as F,
                requests: options.query.requests.map(({request, range}) => {
                    return {
                        range,
                        request: {
                            includeAllBlocks: request.includeAllBlocks,
                            transactions: request.transactions,
                            traces: request.traces,
                            stateDiffs: request.stateDiffs,
                            logs: [
                                ...(request.logs ?? []),
                                ...(request.transfers?.map(({from, to, ...rest}) => ({
                                    topic0: [erc20.events.Transfer.topic],
                                    topic1: from ? from.map(addressToTopic) : undefined,
                                    topic2: to ? to.map(addressToTopic) : undefined,
                                    ...rest,
                                })) ?? []),
                            ],
                        },
                    }
                }),
            },
        })
        this.query = options.query
    }

    getHeight(): Promise<number> {
        return this.ds.getHeight()
    }

    getFinalizedHeight(): Promise<number> {
        return this.ds.getFinalizedHeight()
    }

    getBlockStream(range?: Range, stopOnHead?: boolean): ReadableStream<any> {
        return this.ds.getBlockStream(range, stopOnHead).pipeThrough(
            new TransformStream({
                transform: ({blocks}, controller) => {
                    let request = getRequestAt(this.query.requests, (blocks[0].header as any).number)
                    let isLogsRequested =
                        !!request?.logs?.length ||
                        !!request?.transactions?.some((t) => t.logs) ||
                        !!request?.traces?.some((t) => t.transactionLogs) ||
                        !!request?.transfers?.some((t) => t.transactionLogs || t.log)

                    controller.enqueue(
                        blocks.map((b) => ({
                            ...b,
                            logs: isLogsRequested ? b.logs : undefined,
                            transfers: !!request?.transfers?.length
                                ? b.logs?.map((l) => {
                                      let log = l as Log
                                      if (!erc20.events.Transfer.is(log)) return

                                      let {data, topics} = log
                                      let event = erc20.events.Transfer.decode({data, topics})

                                      let transfer = {} as Transfer

                                      this.query.fields.transfer?.logIndex && (transfer.logIndex = log.logIndex)
                                      this.query.fields.transfer?.transactionIndex &&
                                          (transfer.transactionIndex = log.transactionIndex)
                                      this.query.fields.transfer?.address && (transfer.address = log.address)
                                      this.query.fields.transfer?.from && (transfer.from = event.from)
                                      this.query.fields.transfer?.to && (transfer.to = event.to)
                                      this.query.fields.transfer?.value && (transfer.value = event.value)

                                      return transfer
                                  })
                                : undefined,
                        }))
                    )
                },
            })
        )
    }
}

function addressToTopic(a: string) {
    return '0x' + a.toLowerCase().slice(2).padStart(64, '0')
}
