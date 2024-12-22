import {PortalClient} from '@subsquid/portal-client'
import {HttpClient} from '@subsquid/http-client'
import {createLogger} from '@subsquid/logger'
import {EvmPortalDataSource, Query} from '@subsquid/evm-stream'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
        http: new HttpClient({
            retryAttempts: Infinity,
        }),
        newBlockTimeout: 5000,
        log: createLogger('portal-client'),
    })

    let query: Query = {
        range: {
            from: 0,
        },
        request: {
            logs: [
                {
                    address: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
                    topic0: ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'],
                },
            ],
        },
    }

    let ds = new EvmPortalDataSource({
        portal,
        query,
        fields: {
            block: {timestamp: true, size: true, number: true, hash: true, parentHash: true},
            transaction: {from: true, to: true, hash: true, transactionIndex: true},
            log: {
                address: true,
                topics: true,
                data: true,
                transactionHash: true,
                logIndex: true,
                transactionIndex: true,
            },
        },
    })

    let from = await ds.getHeight().then((h) => h - 1_000_000)

    for await (let {blocks, finalizedHead} of ds.getBlockStream({from})) {
        console.log(
            `progress: ${blocks[blocks.length - 1].header.height} / ${finalizedHead.height}, ` +
                `blocks: ${blocks.length}, ` +
                `logs: ${blocks.reduce((r, b) => (r += b.logs.length), 0)}`
        )
    }
}

main()
