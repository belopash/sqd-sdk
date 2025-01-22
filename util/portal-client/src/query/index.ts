import {EvmQuery, EvmResponse} from './evm'

export type BaseQuery = {
    type: string
    fromBlock?: number
    toBlock?: number
}

export type PortalQuery = BaseQuery | EvmQuery

export interface HashAndNumber {
    hash: string
    number: number
}

export interface BaseResponse {
    header: HashAndNumber
}

export type PortalResponse<Q extends PortalQuery> = Q extends EvmQuery ? EvmResponse<Q> : BaseResponse
