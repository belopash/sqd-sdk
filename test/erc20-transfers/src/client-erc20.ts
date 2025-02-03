import {PortalClient} from '@subsquid/portal-client'
import {Evm} from '@subsquid/portal-client'
import {HttpClient} from '@subsquid/http-client'

async function main() {
    let portal = new PortalClient({
        url: 'http://localhost:3000',
        http: new HttpClient({
            retryAttempts: Infinity,
        }),
    })

    let fromBlock = await portal.getFinalizedHeight().then((h) => h - 1_000_000)

    // all SQD transfers
    let stream = portal.getFinalizedStream(
        {
            type: 'evm',
            fromBlock,
            fields: {
                block: {
                    timestamp: true,
                },
            },
        } satisfies Evm.FinalizedQuery,
        {
            stopOnHead: true,
        }
    )

    for await (let {blocks} of stream) {
        for (let block of blocks) {
            let transfers = (block as any).transfers as any[]
            for (let transfer of transfers) {
                console.log(
                    `[${new Date((block.header as any).timestamp * 1_000).toISOString()}]` +
                        ` ${transfer.from} -> ${transfer.to} (${transfer.value})`
                )
            }
        }
    }
    console.log(`end`)
}

main()
