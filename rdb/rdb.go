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

	"github.com/soyart/eth-tx-crawler/entity"
)

type RedisWrapper interface {
	SaveTxs(context.Context, entity.EthDatasByBlockNumber) error
	SaveLogTxs(context.Context, map[string]entity.EthDatasByBlockNumber) error

	SetLastRecordedBlock(context.Context, uint64) error
	GetLastRecordedBlock(context.Context) (uint64, error)
}

type redisWrapper struct {
	db *redis.Client
}

func New(redisUrl string) (RedisWrapper, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisUrl,
	})

	if rdb == nil {
		return nil, errors.New("got nil redis client")
	}

	return &redisWrapper{db: rdb}, nil
}

func txDataKey() string {
	return "ethtxrawler:txs"
}

func logDataKey(addr string) string {
	return "ethtxrawler:logs:" + addr
}

func lastRecordedBlockKey() string {
	return "ethtxrawler:lastRecordedBlock"
}

func (rdw *redisWrapper) SaveTxs(ctx context.Context, dataByBlockNumber entity.EthDatasByBlockNumber) error {
	var wg sync.WaitGroup
	wg.Add(len(dataByBlockNumber))
	errChan := make(chan error)

	key := txDataKey()
	for blockNumber, txDatas := range dataByBlockNumber {
		go func(number uint64, datas []entity.EthData) {
			defer wg.Done()

			dataJson, err := json.Marshal(datas)
			if err != nil {
				errChan <- errors.Wrap(err, "failed to marshal to json")
			}

			if err := rdw.db.HSet(ctx, key, number, dataJson).Err(); err != nil {
				errChan <- errors.Wrapf(err, "failed to save block %d data", number)
			}
		}(blockNumber, txDatas)
	}

	return concurrent.WaitAndCollectErrors(&wg, errChan)
}

func (rdw *redisWrapper) SaveLogTxs(ctx context.Context, dataByAddr map[string]entity.EthDatasByBlockNumber) error {
	errChan := make(chan (error))
	var wg sync.WaitGroup
	wg.Add(len(dataByAddr))

	for ethAddr, ethDatasByBlock := range dataByAddr {
		go func(addr string, datas entity.EthDatasByBlockNumber) {
			defer wg.Done()

			key := logDataKey(addr)

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
	blockString := fmt.Sprintf("%d", block)
	if err := rdw.db.Set(ctx, lastRecordedBlockKey(), blockString, 0).Err(); err != nil {
		return errors.Wrapf(err, "failed to set lastRecordedBlock %d", block)
	}

	return nil
}

func (rdw *redisWrapper) GetLastRecordedBlock(ctx context.Context) (uint64, error) {
	blockString, err := rdw.db.Get(ctx, lastRecordedBlockKey()).Result()
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
