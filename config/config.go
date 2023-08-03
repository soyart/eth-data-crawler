package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/soyart/gsl/soyutils"
)

type Mode int

const (
	ModeTxs Mode = iota
	ModeLogTxs
)

func (m Mode) String() string {
	switch m {
	case ModeTxs:
		return "txs"
	case ModeLogTxs:
		return "log-txs"
	default:
		panic(fmt.Sprintf("bad mode: %d", m))
	}
}

type Config struct {
	ModeConfig string `yaml:"mode" json:"-"`
	Mode       Mode   `yaml:"-" json:"mode"` // Will be parsed based on ModeConfig

	NodeUrl  string `yaml:"node_url" json:"nodeUrl"`
	RedisUrl string `yaml:"redis_url" json:"redisUrl"`

	Addresses []common.Address `yaml:"addresses" json:"addresses"`

	FromBlock uint64 `yaml:"from_block" json:"fromBlock"`
	ToBlock   uint64 `yaml:"to_block" json:"toBlock"`
	BatchSize uint64 `yaml:"batch_size" json:"batchSize"`
}

func From(filename string) (*Config, error) {
	conf, err := soyutils.ReadFileYAMLPointer[Config](filename)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", filename)
	}

	// Defaults too 100 blocks
	if conf.ToBlock == 0 {
		conf.ToBlock = conf.FromBlock + 100
	}

	// Defaults to 25 blocks
	if conf.BatchSize == 0 {
		conf.BatchSize = 25
	}

	// Allow env override for NodeUrl
	nodeUrl, found := os.LookupEnv("NODE_URL")
	if found {
		conf.NodeUrl = nodeUrl
	}

	if conf.NodeUrl == "" {
		return nil, errors.New("empty ethereum node url")
	}

	// Allow env override for RedisUrl
	redisUrl, found := os.LookupEnv("REDIS_URL")
	if found {
		// Strip protocol string
		if strings.Contains(redisUrl, "redis://") {
			urlParts := strings.Split(redisUrl, "redis://")
			if len(urlParts) < 2 {
				return nil, fmt.Errorf("bad REDIS_URL env %s", redisUrl)
			}

			redisUrl = urlParts[1]
		}

		conf.RedisUrl = redisUrl
	}

	if conf.RedisUrl == "" {
		return nil, errors.New("empty redis url")
	}

	switch conf.ModeConfig {
	case "log", "log-txs", "logs", "logs-txs":
		conf.Mode = ModeLogTxs
	default:
		conf.Mode = ModeTxs
	}

	return conf, nil
}
