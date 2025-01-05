export type BaseQuery = {
    type: string
    fromBlock: number
    toBlock?: number
    [key: string]: any
}
