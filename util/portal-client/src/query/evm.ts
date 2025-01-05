import {BaseQuery} from './base'
import type {Simplify} from '@subsquid/util-types'

export type EvmQuery = Simplify<
    BaseQuery & {
        type: 'evm'
        fields: EvmQueryFieldSelection
        logs: any[]
        transactions: any[]
        traces: any[]
        statediffs: any[]
        includeAllBlocks?: boolean
    }
>

export type EvmQueryFieldSelection = {
    block?: EvmQueryBlockFields
    transaction?: EvmQeuryTransactionFields
    log?: EvmQeuryLogFields
}

export type EvmQueryBlockFields = {
    number?: boolean
    hash?: boolean
    parentHash?: boolean
    nonce?: boolean
    sha3Uncles?: boolean
    logsBloom?: boolean
    transactionsRoot?: boolean
    stateRoot?: boolean
    receiptsRoot?: boolean
    mixHash?: boolean
    miner?: boolean
    difficulty?: boolean
    totalDifficulty?: boolean
    extraData?: boolean
    size?: boolean
    gasLimit?: boolean
    gasUsed?: boolean
    timestamp?: boolean
    baseFeePerGas?: boolean

    l1BlockNumber?: boolean
}

export type EvmQeuryTransactionFields = {
    transactionIndex?: boolean
    sighash?: boolean
    hash?: boolean
    from?: boolean
    to?: boolean
    gas?: boolean
    gasPrice?: boolean
    maxFeePerGas?: boolean
    maxPriorityFeePerGas?: boolean
    input?: boolean
    nonce?: boolean
    value?: boolean
    v?: boolean
    r?: boolean
    s?: boolean
    yParity?: boolean
    chainId?: boolean
    authorizationList?: boolean
    gasUsed?: boolean
    cumulativeGasUsed?: boolean
    effectiveGasPrice?: boolean
    contractAddress?: boolean
    type?: boolean
    status?: boolean

    l1Fee?: boolean
    l1FeeScalar?: boolean
    l1GasPrice?: boolean
    l1GasUsed?: boolean
    l1BlobBaseFee?: boolean
    l1BlobBaseFeeScalar?: boolean
    l1BaseFeeScalar?: boolean
}

export type EvmQeuryLogFields = {
    logIndex?: boolean
    transactionIndex?: boolean
    transactionHash?: boolean
    address?: boolean
    data?: boolean
    topics?: boolean
}
