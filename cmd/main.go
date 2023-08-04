package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/soyart/eth-tx-crawler/config"
	"github.com/soyart/eth-tx-crawler/entity"
	"github.com/soyart/eth-tx-crawler/rdb"
)

func panicf(fmtString string, vars ...interface{}) {
	panic(fmt.Sprintf(fmtString, vars...))
}

func main() {
	configFile := "./config/config.yaml"

	conf, err := config.From(configFile)
	if err != nil {
		panicf("failed to read config %s: %s", configFile, err.Error())
	}

	logger, err := zap.NewProduction(zap.Fields(zap.String("serviceLabel", conf.Label), zap.String("mode", conf.Mode.String())))
	if err != nil {
		panicf("failed to init logger: %s", err.Error())
	}

	confJson, err := json.Marshal(conf)
	if err != nil {
		panicf("failed to json marshal conf: %s", err.Error())
	}

	logger.Info("config", zap.String("values", string(confJson)))

	ctx := context.Background()
	client, err := ethclient.DialContext(ctx, conf.NodeUrl)
	if err != nil {
		panicf("failed to dial http node %s: %s", conf.NodeUrl, err.Error())
	}

	logger.Info("created new ethclient", zap.String("url", conf.NodeUrl))

	rdw, err := rdb.New(conf.RedisUrl, conf.Label, logger)
	if err != nil {
		panicf("failed to create new redis wrapper client on %s: %s", conf.RedisUrl, err.Error())
	}

	logger.Info("created new redis client wrapper", zap.String("url", conf.RedisUrl))

	logger.Info("starting main loop")

	switch conf.Mode {
	case config.ModeTxs:
		if err := getAndSaveTxs(
			ctx,
			logger,
			client,
			rdw,
			conf.FromBlock,
			conf.ToBlock,
			conf.BatchSize,
			conf.Rolling,
		); err != nil {
			logger.Error("got error in main loop", zap.String("error", err.Error()))

			panicf("main loop failed: %s", err.Error())
		}

	case config.ModeLogTxs:
		if err := getAndSaveLogsTxs(
			ctx,
			logger,
			client,
			rdw,
			conf.Addresses,
			conf.FromBlock,
			conf.ToBlock,
			conf.BatchSize,
			conf.Rolling,
		); err != nil {
			logger.Error("got error in main loop", zap.String("error", err.Error()))

			panicf("main loop failed: %s", err.Error())
		}
	}
}

func getAndSaveTxs(
	ctx context.Context,
	logger *zap.Logger,
	client *ethclient.Client,
	rdw rdb.RedisWrapper,
	fromBlock uint64,
	toBlock uint64,
	batchSize uint64,
	rolling bool,
) error {
	lastRecordedBlock, err := rdw.GetLastRecordedBlock(ctx)
	if err != nil {
		return errors.Wrap(err, "redis error")
	}

	// First run
	var firstRun bool
	if lastRecordedBlock == 0 {
		firstRun = true
		lastRecordedBlock = fromBlock
	}

	logger.Info("starting looping", zap.Uint64("lastRecordedBlock", lastRecordedBlock), zap.Uint64("toBlock", toBlock))

	for rolling || lastRecordedBlock < toBlock {
		var thisFromBlock uint64
		if firstRun {
			thisFromBlock = lastRecordedBlock
		} else {
			thisFromBlock = lastRecordedBlock + 1
		}

		thisToBlock := thisFromBlock + batchSize

		currentBlock, err := client.BlockNumber(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get current block number")
		}

		// Chop to most current block
		if thisToBlock > currentBlock {
			thisToBlock = currentBlock
		}

		if !rolling && thisToBlock > toBlock {
			thisToBlock = toBlock
		}

		numberOfBlocks := int(thisToBlock - thisFromBlock + 1)
		logger.Info("new loop", zap.Int("expected number of blocks", numberOfBlocks), zap.Uint64("lastRecordedBlock", lastRecordedBlock), zap.Uint64("thisFromBlock", thisFromBlock), zap.Uint64("thisToBlock", thisToBlock))

		blockChan := make(chan *types.Block, numberOfBlocks) // Buffered to avoid clogging
		errChan := make(chan error)

		var wg sync.WaitGroup
		wg.Add(numberOfBlocks)
		for blockNumber := thisFromBlock; blockNumber <= thisToBlock; blockNumber++ {
			go func(n uint64) {
				defer wg.Done()

				block, err := client.BlockByNumber(ctx, big.NewInt(int64(n)))
				if err != nil {
					errChan <- errors.Wrapf(err, "failed to get block %d data", n)
				}

				blockChan <- block
			}(blockNumber)
		}

		wg.Wait()

		dataByBlock := make(entity.EthDatasByBlockNumber)

		var c int
		for c < numberOfBlocks {
			select {
			case err := <-errChan:
				return errors.Wrap(err, "got error from errChan")
			case block := <-blockChan:
				logger.Info("got block", zap.Uint64("number", block.NumberU64()))

				txs := block.Transactions()
				datas := make([]entity.EthData, len(txs))

				for i := range txs {
					tx := txs[i]

					datas[i] = entity.EthData{
						Hash: strings.ToLower(tx.Hash().Hex()),
						Data: hex.EncodeToString((tx.Data())),
					}
				}

				dataByBlock[block.NumberU64()] = datas
				c++
			}
		}

		logger.Info("saving to redis", zap.Int("len", len(dataByBlock)), zap.Uint64("thisFromBlock", thisFromBlock), zap.Uint64("thisToBlock", thisToBlock))

		if err := rdw.SaveTxs(ctx, dataByBlock); err != nil {
			return errors.Wrap(err, "failed to save tx data to redis")
		}

		if err := rdw.SetLastRecordedBlock(ctx, thisToBlock); err != nil {
			return errors.Wrap(err, "failed to save lastRecordedBlock")
		}

		lastRecordedBlock = thisToBlock
	}

	return nil
}

func getAndSaveLogsTxs(
	ctx context.Context,
	logger *zap.Logger,
	client *ethclient.Client,
	rdw rdb.RedisWrapper,
	addresses []common.Address,
	fromBlock uint64,
	toBlock uint64,
	batchSize uint64,
	rolling bool,
) error {
	lastRecordedBlock, err := rdw.GetLastRecordedBlock(ctx)
	if err != nil {
		return errors.Wrap(err, "redis error")
	}

	// First run
	var firstRun bool
	if lastRecordedBlock == 0 {
		firstRun = true
		lastRecordedBlock = fromBlock
	}

	logger.Info("starting looping", zap.Uint64("lastRecordedBlock", lastRecordedBlock), zap.Uint64("toBlock", toBlock))

	for rolling || lastRecordedBlock < toBlock {
		var thisFromBlock uint64
		if firstRun {
			thisFromBlock = lastRecordedBlock
		} else {
			thisFromBlock = lastRecordedBlock + 1
		}

		thisToBlock := thisFromBlock + batchSize

		currentBlock, err := client.BlockNumber(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get current block number")
		}

		// Chop to most current block
		if thisToBlock > currentBlock {
			thisToBlock = currentBlock
		}

		if !rolling && thisToBlock > toBlock {
			thisToBlock = toBlock
		}

		numberOfBlocks := int(thisToBlock - thisFromBlock + 1)
		logger.Info("new loop", zap.Int("expected number of blocks", numberOfBlocks), zap.Uint64("lastRecordedBlock", lastRecordedBlock), zap.Uint64("thisFromBlock", thisFromBlock), zap.Uint64("thisToBlock", thisToBlock))

		logs, err := client.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: big.NewInt(int64(thisFromBlock)),
			ToBlock:   big.NewInt(int64(thisToBlock)),
			Addresses: addresses,
		})
		if err != nil {
			return errors.Wrap(err, "failed to filter logs")
		}

		logger.Info("got logs", zap.Int("len", len(logs)))

		dataByAddr := make(map[string]entity.EthDatasByBlockNumber)
		for i := range logs {
			log := logs[i]

			addr := strings.ToLower(log.Address.Hex())
			hash := strings.ToLower(log.TxHash.Hex())
			data := strings.ToLower(hex.EncodeToString(log.Data))

			contractData := dataByAddr[addr]
			if contractData == nil {
				contractData = make(entity.EthDatasByBlockNumber)
				dataByAddr[addr] = contractData
			}

			blockDatas := contractData[log.BlockNumber]
			if blockDatas == nil {
				blockDatas = make([]entity.EthData, 0, 1)
				contractData[log.BlockNumber] = blockDatas
			}

			blockDatas = append(blockDatas, entity.EthData{
				Hash: hash,
				Data: data,
			})

			dataByAddr[addr][log.BlockNumber] = blockDatas
		}

		logger.Info("saving to redis", zap.Int("len", len(dataByAddr)), zap.Uint64("thisFromBlock", thisFromBlock), zap.Uint64("thisToBlock", thisToBlock))
		if err := rdw.SaveLogTxs(ctx, dataByAddr); err != nil {
			return errors.Wrapf(err, "failed to save data in range %d - %d", lastRecordedBlock, thisToBlock)
		}

		logger.Info("saving lastRecordedBlock")
		if err := rdw.SetLastRecordedBlock(ctx, thisToBlock); err != nil {
			return errors.Wrapf(err, "failed to save lastRecordedBlock in range %d - %d", lastRecordedBlock, thisToBlock)
		}

		lastRecordedBlock = thisToBlock
	}

	return nil
}
