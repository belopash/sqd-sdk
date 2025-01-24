import fastify from 'fastify'
import {PortalClient} from '@subsquid/portal-client'
import {HttpClient} from '@subsquid/http-client'
import {Erc20DataRequest, ERC20DataSource, Erc20FieldSelection} from './source'

const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/arbitrum-one',
    http: new HttpClient({
        retryAttempts: 3,
    }),
    minBytes: 1 * 1024 * 1024,
})

const app = fastify({
    logger: true,
})

app.setErrorHandler((err, req, reply) => {
    console.error(err)
    reply.status(500).send()
})

type StreamRequest = {
    type: 'erc20'
    fromBlock?: number
    toBlock?: number
    fields: Erc20FieldSelection
} & Erc20DataRequest

app.get('/finalized-stream/height', async (req, reply) => {
    try {
        let height = await portal.getFinalizedHeight()
        reply.send(height.toString())
    } catch (e) {
        console.error(e)
        reply.status(500).send()
    }
})

app.post('/finalized-stream', async (req, reply) => {
    let {type, fromBlock, toBlock, fields, ...request} = req.body as StreamRequest
    let dataSource = new ERC20DataSource({
        portal,
        query: {
            fields,
            requests: [{range: {from: fromBlock ?? 0, to: toBlock}, request}],
        },
    })

    let stream = dataSource.getBlockStream({from: 0}, true).pipeThrough(
        new TransformStream({
            transform: ({blocks}, controller) => {
                let res = ''
                for (let block of blocks) {
                    let json = JSON.stringify(block, (_, value) =>
                        typeof value === 'bigint' ? value.toString() : value
                    )
                    res += json + '\n'
                }
                controller.enqueue(res)
            },
        })
    )

    await reply.send(stream)
})

const port = 3000
app.listen({port}, (err, address) => {
    if (err) throw err
    console.log(`Server is running at ${address}`)
})
