import {PortalClient, PortalQuery, PortalResponse} from '@subsquid/portal-client'
import {HttpClient} from '@subsquid/http-client'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
        http: new HttpClient({
            retryAttempts: Infinity,
            // bodyTimeout: 10
        }),
    })

    let fromBlock = await portal.getFinalizedHeight().then((h) => h - 1_000_000)

    let query = {
        type: 'evm',
        fromBlock,
        fields: {
            block: {
                number: true,
                hash: true,
                parentHash: true,
            },
            transaction: {
                from: true,
                to: true,
                hash: true,
                transactionIndex: true,
            },
            log: {
                address: true,
                topics: true,
                data: true,
                transactionHash: true,
                logIndex: true,
                transactionIndex: true,
            },
            stateDiff: {
                kind: true,
                next: true,
                prev: true,
            },
        },
        logs: [
            {
                address: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
                topic0: ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'],
            },
        ],
        stateDiffs: [],
        traces: [],
        includeAllBlocks: false,
        transactions: [],
    } satisfies PortalQuery

    for await (let {blocks, finalizedHead} of portal.getFinalizedStream(query)) {
        blocks[0].header
        console.log(
            `progress: ${blocks[blocks.length - 1].header.number} / ${finalizedHead.number}, ` +
                `blocks: ${blocks.length}, ` +
                `logs: ${blocks.reduce((r, b) => (r += b.logs.length), 0)}`
        )
    }
}

main()
