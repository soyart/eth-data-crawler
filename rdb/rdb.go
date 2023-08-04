package rdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/soyart/gsl/concurrent"
	"go.uber.org/zap"

	"github.com/soyart/eth-tx-crawler/entity"
)

const keyPrefix = "ethtxcrawler"

type RedisWrapper interface {
	// Save tx data to Redis as strings (JSON arrays)
	SaveTxs(context.Context, entity.EthDatasByBlockNumber) error
	// Save tx data to Redis as hashes, with keys being contract addr and sub-keys being block numbers.
	SaveLogTxs(context.Context, map[string]entity.EthDatasByBlockNumber) error

	SetLastRecordedBlock(context.Context, uint64) error
	GetLastRecordedBlock(context.Context) (uint64, error)
}

type redisWrapper struct {
	// Allow multiple services to use the same Redis databases
	serviceLabel string
	mode         string
	db           *redis.Client
	logger       *zap.Logger
}

func New(redisUrl string, label string, mode string, logger *zap.Logger) (RedisWrapper, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisUrl,
	})

	if rdb == nil {
		return nil, errors.New("got nil redis client")
	}

	switch label {
	case "":
		label = keyPrefix
	default:
		label = fmt.Sprintf("%s@%s", keyPrefix, label)
	}

	return &redisWrapper{
		db:           rdb,
		serviceLabel: label,
		mode:         mode,
		logger:       logger,
	}, nil
}

func (rdw *redisWrapper) txDataKey() string {
	return keyPrefix + ":txs"
}

func (rdw *redisWrapper) logDataKey(addr string) string {
	return fmt.Sprintf("%s:logs:%s", rdw.serviceLabel, addr)
}

func (rdw *redisWrapper) lastRecordedBlockKey() string {
	return fmt.Sprintf("%s:%s:lastRecordedBlock", rdw.serviceLabel, rdw.mode)
}

func (rdw *redisWrapper) SaveTxs(ctx context.Context, dataByBlockNumber entity.EthDatasByBlockNumber) error {
	var wg sync.WaitGroup
	wg.Add(len(dataByBlockNumber))
	errChan := make(chan error)

	key := rdw.txDataKey()
	for blockNumber, txDatas := range dataByBlockNumber {
		go func(k string, n uint64, datas []entity.EthData) {
			defer wg.Done()

			dataJson, err := json.Marshal(datas)
			if err != nil {
				errChan <- errors.Wrap(err, "failed to marshal to json")
			}

			if err := rdw.db.HSet(
				ctx,
				key,
				strconv.FormatUint(n, 10),
				dataJson,
			).Err(); err != nil {
				errChan <- errors.Wrapf(err, "failed to save key %s to redis", key)
			}
		}(key, blockNumber, txDatas)
	}

	return concurrent.WaitAndCollectErrors(&wg, errChan)
}

func (rdw *redisWrapper) SaveLogTxs(ctx context.Context, dataByAddr map[string]entity.EthDatasByBlockNumber) error {
	var wg sync.WaitGroup
	wg.Add(len(dataByAddr))
	errChan := make(chan (error))

	for ethAddr, ethDatasByBlock := range dataByAddr {
		go func(addr string, datas entity.EthDatasByBlockNumber) {
			defer wg.Done()

			key := rdw.logDataKey(addr)
			for blockNumber, ethData := range datas {
				data, err := json.Marshal(ethData)
				if err != nil {
					errChan <- errors.Wrap(err, "failed to marshal ethdatas to json")
				}

				if err := rdw.db.HSet(ctx, key, blockNumber, data).Err(); err != nil {
					errChan <- errors.Wrapf(err, "failed to save key %s to redis", key)
				}
			}
		}(ethAddr, ethDatasByBlock)
	}

	return concurrent.WaitAndCollectErrors(&wg, errChan)
}

func (rdw *redisWrapper) SetLastRecordedBlock(ctx context.Context, block uint64) error {
	blockString := strconv.FormatUint(block, 10)

	if err := rdw.db.Set(ctx, rdw.lastRecordedBlockKey(), blockString, 0).Err(); err != nil {
		return errors.Wrapf(err, "failed to set lastRecordedBlock %d", block)
	}

	return nil
}

func (rdw *redisWrapper) GetLastRecordedBlock(ctx context.Context) (uint64, error) {
	blockString, err := rdw.db.Get(ctx, rdw.lastRecordedBlockKey()).Result()
	if err != nil {
		// First run
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, errors.Wrap(err, "failed to get lastRecordedBlock")
	}

	block, err := strconv.ParseUint(blockString, 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse redis string to block number")
	}

	return block, nil
}
