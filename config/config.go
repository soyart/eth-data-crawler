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
	Label      string `yaml:"label" json:"label"`
	ModeConfig string `yaml:"mode" json:"-"`
	Mode       Mode   `yaml:"-" json:"mode"` // Will be parsed based on ModeConfig
	Rolling    bool   `yaml:"rolling" json:"rolling"`

	NodeUrl  string `yaml:"node_url" json:"nodeUrl"`
	RedisUrl string `yaml:"redis_url" json:"redisUrl"`

	Addresses []common.Address `yaml:"addresses" json:"addresses"`

	FromBlock uint64 `yaml:"from_block" json:"fromBlock"`
	ToBlock   uint64 `yaml:"to_block" json:"toBlock"`
	BatchSize uint64 `yaml:"batch_size" json:"batchSize"`
}

func From(filename string) (*Config, error) {
	if envFilename, found := os.LookupEnv("CONF_FILE"); found {
		filename = envFilename
	}

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
	if nodeUrl, found := os.LookupEnv("NODE_URL"); found {
		conf.NodeUrl = nodeUrl
	}

	if conf.NodeUrl == "" {
		return nil, errors.New("empty ethereum node url")
	}

	// Allow env override for RedisUrl
	if redisUrl, found := os.LookupEnv("REDIS_URL"); found {
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

	conf.Mode = chooseMode(conf.ModeConfig)

	if mode, found := os.LookupEnv("MODE"); found {
		conf.Mode = chooseMode(mode)
	}

	if label, found := os.LookupEnv("LABEL"); found {
		conf.Label = label
	}

	if rolling, found := os.LookupEnv("ROLLING"); found {
		switch strings.ToLower(rolling) {
		case "1", "true", "yes":
			conf.Rolling = true
		case "0", "false", "no":
			conf.Rolling = false

		default:
			return nil, fmt.Errorf("illegal ROLLING flag: %s", rolling)
		}
	}

	return conf, nil
}

func chooseMode(modeConfig string) Mode {
	switch modeConfig {
	case "2", "log", "log-txs", "logs", "logs-txs":
		return ModeLogTxs
	default:
		return ModeTxs
	}
}
