# eth-tx-crawler

> The Ethereum node URL provided in [`config.yaml.example`](./config/config.yaml.example)
> and in [`docker-compose.yaml`](./deploy/docker-compose.yaml)
> is not a full/archive node, so it will not have data from old blocks.
>
> If you need old block data, supply your own Ethereum node URL, either via
> the config file or via environment `NODE_URL`.

A simple Ethereum data crawler service.

It has 2 modes of operations: (1) transactions (default), and (2) event logs.
Both modes filter in an infinite loop, each time filtering a fixed
number of blocks, configurable as `batch_size` in `config/config.yaml`.

eth-tx-crawler keeps a state in Redis - `ethtxcrawler:lastRecordedBlock`, which represents
the tallest block the service has filtered.

Unless config `rolling` was set to true, eth-tx-crawler crawls for data until
a *to block*, at which point it exits.

> Caveat: eth-tx-crawler does NOT handle [chain reorg](https://www.alchemy.com/overviews/what-is-a-reorg)

## Environment variables

- `NODE_URL`

  specifies `go-ethereum`-compatible JSON RPC node URL

- `REDIS_URL`

  specifies Redis connection URL

- `CONF_FILE`

  specifies YAML configuration to use

- `LABEL`

  specifies service label when scaling or running multiple instances
  of eth-tx-crawler

- `ROLLING`

  if set to true, eth-tx-crawler will not exit upon reaching *to block*.

## All transaction mode

Use this mode (default) if the transactions in questions
do not emit event logs.

In transactions mode, the service just simply repeatedly
calls `ethclient.Client.BlockByNumber`. This implies that
this mode does record all transactions data on that block.

It then aggregates the results, and saves data to Redis as
[Redis hashes](https://redis.io/docs/data-types/hashes/).

The Redis key for this is `ethtxcrawler:${blockNumber}:txs`,
so that it could be easy for us to later get specific TXs from specific blocks

## Contract event log mode

To use this mode, change the configuration or env `MODE`
to `2`, `log`, `logs`, `log-txs`, or `logs-txs`

Use this mode if the transactions in questions
do emit event logs, as we would have better indexing
capability (by contract address).

With this mode, users can configure specific contracts
to get logs from. Log topics are not yet supported.

It saves data to Redis as [Redis hashes](https://redis.io/docs/data-types/hashes/),
with [keys being concatenated contract addresses `ethtxcrawler:${contractAddr}:logs`,
and subkeys (hash keys) being block numbers](./rdb/rdb.go).

This allows other services to quickly aggregate and access
contract transaction data.

## Configuring

This service can be configured via `./config/config.yaml`,
and environment variable `NODE_URL` and `REDIS_URL`

Users can configure contract addresses config file

## Deploying

This repository provides a [Dockerfile](./Dockerfile)
and a [`Docker Compose YAML`](./deploy/docker-compose.yaml)
for quick deployment.

```shell
docker compose -f ./deploy/docker-compose.yaml up -d
```

The image is also available on Docker Hub
repository [`artnoi/eth-tx-crawler`](https://hub.docker.com/r/artnoi/eth-tx-crawler).

## Scaling

Horizontal scaling is possible by using multiple different instances eth-data-crawler
to track different contracts.

Because Ethereum addresses are cryptographically random, we can be
confident that the distribution of the hex addresses are uniform.
This confidence allows us to separate each Redis databases to a range of contracts,
separated by addresses alphabetically for [contract event log mode](#contract-event-log-mode).

By having different pools for each different contract ranges (i.e. host `redis1`
for contracts `0x0-0x3`, `redis2` for `0x3-0x7`, `redis3` for `0x8-0x11`,
`redis4` for `0x12-0x15`), we can scale this pattern horizontally.

Because eth-tx-crawler keeps state `lastRecordedBlock` state in Redis,
we provide a way to label this state in Redis, so that each eth-tx-crawler service
has its own correct, exclusive state.
