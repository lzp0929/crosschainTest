package mvcc

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/log"
)

// VersionedValue 表示一个带版本的值
type VersionedValue struct {
	TxIndex   int              // 创建此版本的交易索引
	Value     interface{}      // 值
	IsAborted bool             // 是否已中止
	Readers   map[int]struct{} // 读取此版本的交易集合
}

// MVCCStateManager 管理多版本并发控制状态
type MVCCStateManager struct {
	// 地址状态表: 地址 -> 字段 -> 版本列表
	addressStates map[common.Address]map[string][]VersionedValue

	// 存储状态表: 地址 -> 存储键 -> 版本列表
	storageStates map[common.Address]map[common.Hash][]VersionedValue

	// 读依赖表: 交易索引 -> 依赖的交易索引集合
	readDependencies map[int]map[int]struct{}

	// 交易状态表: 交易索引 -> 状态 (committed, aborted)
	txStatus map[int]string

	// 中止的交易集合
	abortedTxs map[int]struct{}

	// 交易总数
	txCount int

	// 区块号
	blockNumber uint64

	// 用于保护map的并发访问的锁
	mu sync.Mutex
}

// NewMVCCStateManager 创建一个新的MVCC状态管理器
func NewMVCCStateManager(blockNumber int) *MVCCStateManager {
	return &MVCCStateManager{
		addressStates:    make(map[common.Address]map[string][]VersionedValue),
		storageStates:    make(map[common.Address]map[common.Hash][]VersionedValue),
		readDependencies: make(map[int]map[int]struct{}),
		txStatus:         make(map[int]string),
		abortedTxs:       make(map[int]struct{}),
		blockNumber:      uint64(blockNumber),
	}
}

// ReadStorageState 读取存储状态
func (m *MVCCStateManager) ReadStorageState(txIndex int, addr common.Address, key common.Hash, baseState *state.StateDB) (interface{}, bool) {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		return nil, false
	}

	// 尝试从多版本状态表中读取
	var latestVersion *VersionedValue

	m.mu.Lock()
	if m.storageStates[addr] != nil && m.storageStates[addr][key] != nil {
		versions := m.storageStates[addr][key]

		// 查找最新的可见版本（小于当前交易索引的最大版本）
		for i := len(versions) - 1; i >= 0; i-- {
			version := versions[i]
			if version.TxIndex < txIndex && !version.IsAborted {
				latestVersion = &versions[i]
				break
			}
		}

		if latestVersion != nil {
			// 记录读依赖
			if m.readDependencies[txIndex] == nil {
				m.readDependencies[txIndex] = make(map[int]struct{})
			}
			m.readDependencies[txIndex][latestVersion.TxIndex] = struct{}{}

			// 记录读者
			if latestVersion.Readers == nil {
				latestVersion.Readers = make(map[int]struct{})
			}
			latestVersion.Readers[txIndex] = struct{}{}
		}
	}
	m.mu.Unlock()

	if latestVersion != nil {
		return latestVersion.Value, true
	}

	// 如果没有找到合适的版本，从基础状态读取
	baseValue := baseState.GetState(addr, key)

	// 记录对基础状态的读取依赖（使用-1表示基础状态）
	m.mu.Lock()
	if m.readDependencies[txIndex] == nil {
		m.readDependencies[txIndex] = make(map[int]struct{})
	}
	m.readDependencies[txIndex][-1] = struct{}{}
	m.mu.Unlock()

	return baseValue, true
}

// WriteStorageState 写入存储状态
func (m *MVCCStateManager) WriteStorageState(txIndex int, addr common.Address, key common.Hash, value interface{}) bool {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		return false
	}

	// 创建新版本
	newVersion := VersionedValue{
		TxIndex:   txIndex,
		Value:     value,
		IsAborted: false,
		Readers:   make(map[int]struct{}),
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 初始化存储状态表
	if m.storageStates[addr] == nil {
		m.storageStates[addr] = make(map[common.Hash][]VersionedValue)
	}
	if m.storageStates[addr][key] == nil {
		m.storageStates[addr][key] = []VersionedValue{}
	}

	// 检查是否有高序号交易读取了旧版本
	for readerTx := range m.readDependencies {
		if readerTx > txIndex {
			// 检查高序号交易是否依赖于当前交易之前的版本
			for depTx := range m.readDependencies[readerTx] {
				if depTx < txIndex {
					// 高序号交易读取了旧版本，需要中止该交易
					m.AbortTransaction(readerTx)
					log.Debug("中止高序号交易，因为它读取了旧版本",
						"writer", txIndex,
						"reader", readerTx,
						"dependency", depTx)
				}
			}
		}
	}

	// 检查是否有高序号交易已经读取了此地址和键的值
	if m.storageStates[addr][key] != nil {
		for _, version := range m.storageStates[addr][key] {
			for readerTx := range version.Readers {
				if readerTx > txIndex {
					// 高序号交易读取了此地址和键的值，需要中止该交易
					m.AbortTransaction(readerTx)
					log.Debug("中止高序号交易，因为它读取了将被覆盖的值",
						"writer", txIndex,
						"reader", readerTx)
				}
			}
		}
	}

	// 添加新版本
	m.storageStates[addr][key] = append(m.storageStates[addr][key], newVersion)

	// 标记交易为已提交
	m.txStatus[txIndex] = "committed"

	return true
}

// ReadAddressState 读取地址状态
func (m *MVCCStateManager) ReadAddressState(txIndex int, addr common.Address, field string, baseState *state.StateDB) (interface{}, bool) {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		return nil, false
	}

	// 尝试从多版本状态表中读取
	var latestVersion *VersionedValue

	m.mu.Lock()
	if m.addressStates[addr] != nil && m.addressStates[addr][field] != nil {
		versions := m.addressStates[addr][field]

		// 查找最新的可见版本（小于当前交易索引的最大版本）
		for i := len(versions) - 1; i >= 0; i-- {
			version := versions[i]
			if version.TxIndex < txIndex && !version.IsAborted {
				latestVersion = &versions[i]
				break
			}
		}

		if latestVersion != nil {
			// 记录读依赖
			if m.readDependencies[txIndex] == nil {
				m.readDependencies[txIndex] = make(map[int]struct{})
			}
			m.readDependencies[txIndex][latestVersion.TxIndex] = struct{}{}

			// 记录读者
			if latestVersion.Readers == nil {
				latestVersion.Readers = make(map[int]struct{})
			}
			latestVersion.Readers[txIndex] = struct{}{}
		}
	}
	m.mu.Unlock()

	if latestVersion != nil {
		return latestVersion.Value, true
	}

	// 如果没有找到合适的版本，从基础状态读取
	var baseValue interface{}
	switch field {
	case "balance":
		baseValue = baseState.GetBalance(addr)
	case "nonce":
		baseValue = baseState.GetNonce(addr)
	case "code":
		baseValue = baseState.GetCode(addr)
	case "codehash":
		baseValue = baseState.GetCodeHash(addr)
	case "suicided":
		baseValue = baseState.HasSuicided(addr)
	default:
		return nil, false
	}

	// 记录对基础状态的读取依赖（使用-1表示基础状态）
	m.mu.Lock()
	if m.readDependencies[txIndex] == nil {
		m.readDependencies[txIndex] = make(map[int]struct{})
	}
	m.readDependencies[txIndex][-1] = struct{}{}
	m.mu.Unlock()

	return baseValue, true
}

// WriteAddressState 写入地址状态
func (m *MVCCStateManager) WriteAddressState(txIndex int, addr common.Address, field string, value interface{}) bool {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		return false
	}

	// 创建新版本
	newVersion := VersionedValue{
		TxIndex:   txIndex,
		Value:     value,
		IsAborted: false,
		Readers:   make(map[int]struct{}),
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 初始化地址状态表
	if m.addressStates[addr] == nil {
		m.addressStates[addr] = make(map[string][]VersionedValue)
	}
	if m.addressStates[addr][field] == nil {
		m.addressStates[addr][field] = []VersionedValue{}
	}

	// 检查是否有高序号交易读取了旧版本
	for readerTx := range m.readDependencies {
		if readerTx > txIndex {
			// 检查高序号交易是否依赖于当前交易之前的版本
			for depTx := range m.readDependencies[readerTx] {
				if depTx < txIndex {
					// 高序号交易读取了旧版本，需要中止该交易
					m.AbortTransaction(readerTx)
					log.Debug("中止高序号交易，因为它读取了旧版本",
						"writer", txIndex,
						"reader", readerTx,
						"dependency", depTx)
				}
			}
		}
	}

	// 检查是否有高序号交易已经读取了此地址和字段的值
	if m.addressStates[addr][field] != nil {
		for _, version := range m.addressStates[addr][field] {
			for readerTx := range version.Readers {
				if readerTx > txIndex {
					// 高序号交易读取了此地址和字段的值，需要中止该交易
					m.AbortTransaction(readerTx)
					log.Debug("中止高序号交易，因为它读取了将被覆盖的值",
						"writer", txIndex,
						"reader", readerTx)
				}
			}
		}
	}

	// 添加新版本
	m.addressStates[addr][field] = append(m.addressStates[addr][field], newVersion)

	// 标记交易为已提交
	m.txStatus[txIndex] = "committed"

	return true
}

// IsTransactionAborted 检查交易是否已中止
func (m *MVCCStateManager) IsTransactionAborted(txIndex int) bool {
	_, isAborted := m.abortedTxs[txIndex]
	return isAborted
}

// AbortTransaction 中止交易
func (m *MVCCStateManager) AbortTransaction(txIndex int) {
	m.abortedTxs[txIndex] = struct{}{}
	m.txStatus[txIndex] = "aborted"

	// 递归中止依赖于此交易的其他交易
	for depTx := range m.readDependencies {
		for tx := range m.readDependencies[depTx] {
			if tx == txIndex {
				m.AbortTransaction(depTx)
			}
		}
	}
}

// GetVersions 获取指定地址和键的所有版本
func (m *MVCCStateManager) GetVersions(addr common.Address, key common.Hash) []VersionedValue {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.storageStates[addr] == nil || m.storageStates[addr][key] == nil {
		return nil
	}
	return m.storageStates[addr][key]
}

// GetAbortedTransactions 获取所有中止的交易
func (m *MVCCStateManager) GetAbortedTransactions() []int {
	m.mu.Lock()
	defer m.mu.Unlock()

	var abortedTxs []int
	for tx := range m.abortedTxs {
		abortedTxs = append(abortedTxs, tx)
	}
	return abortedTxs
}

// GetBlockNumber 获取区块号
func (m *MVCCStateManager) GetBlockNumber() uint64 {
	return m.blockNumber
}

// GetLatestValue 获取最新的值
func (m *MVCCStateManager) GetLatestValue(addr common.Address, key common.Hash) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.storageStates[addr] == nil || m.storageStates[addr][key] == nil {
		return nil
	}

	versions := m.storageStates[addr][key]
	if len(versions) == 0 {
		return nil
	}

	// 查找最新的非中止版本
	for i := len(versions) - 1; i >= 0; i-- {
		if !versions[i].IsAborted {
			return versions[i].Value
		}
	}

	return nil
}

// CommitToState 将最终状态提交到合约状态
func (m *MVCCStateManager) CommitToState(addr common.Address, key common.Hash, stateDB *state.StateDB) {
	latestValue := m.GetLatestValue(addr, key)
	if latestValue != nil {
		stateDB.SetState(addr, key, latestValue.(common.Hash))
	}
}
