package entity

type EthData struct {
	Hash string `json:"hash"` // lowercase hash string
	Data string `json:"data"` // Hex-encoded string
}

// Maps block number to EthData
type EthDatasByBlockNumber map[uint64][]EthData
