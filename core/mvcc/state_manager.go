package mvcc

import (
	"sort"
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
		log.Debug("交易已中止，读取失败",
			"txIndex", txIndex)
		return nil, false
	}

	// 尝试从多版本状态表中读取
	var latestVersion *VersionedValue
	var maxTxIndex = -1

	m.mu.Lock()
	if m.storageStates[addr] != nil && m.storageStates[addr][key] != nil {
		versions := m.storageStates[addr][key]

		// 查找小于当前交易索引的最大版本（确保读取最新可见版本）
		for i := len(versions) - 1; i >= 0; i-- {
			version := versions[i]
			if version.TxIndex < txIndex && !version.IsAborted && version.TxIndex > maxTxIndex {
				latestVersion = &versions[i]
				maxTxIndex = version.TxIndex
				break
			}
		}

		if latestVersion != nil {
			// 检查版本是否有效（值不为nil）
			if latestVersion.Value == nil {
				log.Debug("读取到已中止交易的版本，跳过",
					"reader", txIndex,
					"writer", latestVersion.TxIndex,
					"addr", addr.Hex(),
					"key", key.Hex())
				m.mu.Unlock()
				return nil, false
			}

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

			log.Debug("成功读取版本",
				"reader", txIndex,
				"writer", latestVersion.TxIndex,
				"value", latestVersion.Value)
		}
	}
	m.mu.Unlock()

	if latestVersion != nil {
		return latestVersion.Value, true
	}

	// 如果没有找到合适的版本，从基础状态读取
	baseValue := baseState.GetState(addr, key)

	log.Debug("从基础状态读取",
		"txIndex", txIndex,
		"addr", addr.Hex(),
		"key", key.Hex(),
		"value", baseValue)

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
		log.Debug("交易已中止，写入失败",
			"txIndex", txIndex)
		return false
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

	// 记录所有需要中止的交易
	txsToAbort := make(map[int]struct{})

	// 检查是否有高序号交易读取了旧版本
	for readerTx := range m.readDependencies {
		if readerTx > txIndex {
			// 检查高序号交易是否依赖于当前交易之前的版本
			for depTx := range m.readDependencies[readerTx] {
				if depTx < txIndex {
					// 高序号交易读取了旧版本，需要中止该交易
					txsToAbort[readerTx] = struct{}{}
					log.Debug("标记高序号交易中止，因为它读取了旧版本",
						"writer", txIndex,
						"reader", readerTx,
						"dependency", depTx)
				}
			}
		}
	}

	// 检查是否有交易读取了将被覆盖的版本
	// 找到所有已存在的版本
	existingVersions := m.storageStates[addr][key]

	// 检查是否已存在该交易的版本
	var existingVersionIndex = -1
	for i, version := range existingVersions {
		// 如果找到了该交易的版本
		if version.TxIndex == txIndex {
			existingVersionIndex = i
		}

		// 如果这个版本是当前交易要覆盖的（即版本号小于当前交易）
		if version.TxIndex < txIndex {
			// 检查所有读取了这个版本的交易
			for readerTx := range version.Readers {
				// 如果读取交易的序号大于当前交易，需要中止
				if readerTx > txIndex {
					txsToAbort[readerTx] = struct{}{}
					log.Debug("标记读取交易中止，因为它读取了将被覆盖的版本",
						"writer", txIndex,
						"reader", readerTx,
						"version", version.TxIndex)
				}
			}
		}
	}

	// 中止所有需要中止的交易
	for txToAbort := range txsToAbort {
		m.AbortTransaction(txToAbort)
	}

	// 如果中止了其他交易，可能需要重新检查当前交易是否已被中止
	if len(txsToAbort) > 0 {
		if m.IsTransactionAborted(txIndex) {
			log.Debug("当前交易在中止其他交易过程中被中止",
				"txIndex", txIndex)
			return false
		}
	}

	// 创建或更新版本
	if existingVersionIndex >= 0 {
		// 更新已存在的版本
		existingVersions[existingVersionIndex].Value = value
		existingVersions[existingVersionIndex].IsAborted = false

		log.Debug("更新交易的已存在版本",
			"txIndex", txIndex,
			"addr", addr.Hex(),
			"key", key.Hex(),
			"value", value)
	} else {
		// 创建新版本
		newVersion := VersionedValue{
			TxIndex:   txIndex,
			Value:     value,
			IsAborted: false,
			Readers:   make(map[int]struct{}),
		}

		// 添加新版本 - 确保新版本被添加到版本列表的末尾，使其对后续交易立即可见
		m.storageStates[addr][key] = append(m.storageStates[addr][key], newVersion)

		log.Debug("交易写入新版本",
			"txIndex", txIndex,
			"addr", addr.Hex(),
			"key", key.Hex(),
			"value", value)
	}

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
		log.Debug("交易已中止，读取失败",
			"txIndex", txIndex)
		return nil, false
	}

	// 尝试从多版本状态表中读取
	var latestVersion *VersionedValue
	var maxTxIndex = -1

	m.mu.Lock()
	if m.addressStates[addr] != nil && m.addressStates[addr][field] != nil {
		versions := m.addressStates[addr][field]

		// 查找小于当前交易索引的最大版本（确保读取最新可见版本）
		for i := len(versions) - 1; i >= 0; i-- {
			version := versions[i]
			if version.TxIndex < txIndex && !version.IsAborted && version.TxIndex > maxTxIndex {
				latestVersion = &versions[i]
				maxTxIndex = version.TxIndex
				break
			}
		}

		if latestVersion != nil {
			// 检查版本是否有效（值不为nil）
			if latestVersion.Value == nil {
				log.Debug("读取到已中止交易的版本，跳过",
					"reader", txIndex,
					"writer", latestVersion.TxIndex,
					"addr", addr.Hex(),
					"field", field)
				m.mu.Unlock()
				return nil, false
			}

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

			log.Debug("成功读取版本",
				"reader", txIndex,
				"writer", latestVersion.TxIndex,
				"field", field,
				"value", latestVersion.Value)
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

	log.Debug("从基础状态读取",
		"txIndex", txIndex,
		"addr", addr.Hex(),
		"field", field,
		"value", baseValue)

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
		log.Debug("交易已中止，写入失败",
			"txIndex", txIndex)
		return false
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

	// 记录所有需要中止的交易
	txsToAbort := make(map[int]struct{})

	// 检查是否有高序号交易读取了旧版本
	for readerTx := range m.readDependencies {
		if readerTx > txIndex {
			// 检查高序号交易是否依赖于当前交易之前的版本
			for depTx := range m.readDependencies[readerTx] {
				if depTx < txIndex {
					// 高序号交易读取了旧版本，需要中止该交易
					txsToAbort[readerTx] = struct{}{}
					log.Debug("标记高序号交易中止，因为它读取了旧版本",
						"writer", txIndex,
						"reader", readerTx,
						"dependency", depTx)
				}
			}
		}
	}

	// 检查是否有交易读取了将被覆盖的版本
	// 找到所有已存在的版本
	existingVersions := m.addressStates[addr][field]

	// 检查是否已存在该交易的版本
	var existingVersionIndex = -1
	for i, version := range existingVersions {
		// 如果找到了该交易的版本
		if version.TxIndex == txIndex {
			existingVersionIndex = i
		}

		// 如果这个版本是当前交易要覆盖的（即版本号小于当前交易）
		if version.TxIndex < txIndex {
			// 检查所有读取了这个版本的交易
			for readerTx := range version.Readers {
				// 如果读取交易的序号大于当前交易，需要中止
				if readerTx > txIndex {
					txsToAbort[readerTx] = struct{}{}
					log.Debug("标记读取交易中止，因为它读取了将被覆盖的版本",
						"writer", txIndex,
						"reader", readerTx,
						"version", version.TxIndex)
				}
			}
		}
	}

	// 中止所有需要中止的交易
	for txToAbort := range txsToAbort {
		m.AbortTransaction(txToAbort)
	}

	// 如果中止了其他交易，可能需要重新检查当前交易是否已被中止
	if len(txsToAbort) > 0 {
		if m.IsTransactionAborted(txIndex) {
			log.Debug("当前交易在中止其他交易过程中被中止",
				"txIndex", txIndex)
			return false
		}
	}

	// 创建或更新版本
	if existingVersionIndex >= 0 {
		// 更新已存在的版本
		existingVersions[existingVersionIndex].Value = value
		existingVersions[existingVersionIndex].IsAborted = false

		log.Debug("更新交易的已存在版本",
			"txIndex", txIndex,
			"addr", addr.Hex(),
			"field", field,
			"value", value)
	} else {
		// 创建新版本
		newVersion := VersionedValue{
			TxIndex:   txIndex,
			Value:     value,
			IsAborted: false,
			Readers:   make(map[int]struct{}),
		}

		// 添加新版本 - 确保新版本被添加到版本列表的末尾，使其对后续交易立即可见
		m.addressStates[addr][field] = append(m.addressStates[addr][field], newVersion)

		log.Debug("交易写入新版本",
			"txIndex", txIndex,
			"addr", addr.Hex(),
			"field", field,
			"value", value)
	}

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
	// 如果交易已经被中止，则直接返回
	if _, exists := m.abortedTxs[txIndex]; exists {
		return
	}

	m.abortedTxs[txIndex] = struct{}{}
	m.txStatus[txIndex] = "aborted"

	// 标记该交易在多版本状态表中的所有写入版本为已中止
	// 遍历地址状态表
	for addr, fields := range m.addressStates {
		for field, versions := range fields {
			for i, version := range versions {
				if version.TxIndex == txIndex {
					// 标记为已中止并将值设置为nil
					versions[i].IsAborted = true
					versions[i].Value = nil
					log.Debug("标记地址状态版本为已中止并清除值",
						"txIndex", txIndex,
						"addr", addr.Hex(),
						"field", field)
				}
			}
		}
	}

	// 遍历存储状态表
	for addr, keys := range m.storageStates {
		for key, versions := range keys {
			for i, version := range versions {
				if version.TxIndex == txIndex {
					// 标记为已中止并将值设置为nil
					versions[i].IsAborted = true
					versions[i].Value = nil
					log.Debug("标记存储状态版本为已中止并清除值",
						"txIndex", txIndex,
						"addr", addr.Hex(),
						"key", key.Hex())
				}
			}
		}
	}

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

// GetAbortedTransactions 获取所有被中止的交易
func (m *MVCCStateManager) GetAbortedTransactions() []int {
	m.mu.Lock()
	defer m.mu.Unlock()

	abortedTxs := make([]int, 0, len(m.abortedTxs))
	for tx := range m.abortedTxs {
		abortedTxs = append(abortedTxs, tx)
	}

	// 按交易索引排序
	sort.Ints(abortedTxs)

	log.Debug("获取被中止的交易列表",
		"count", len(abortedTxs),
		"txs", abortedTxs)

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

// ClearAbortedStatus 清除交易的中止状态，用于重新执行
func (m *MVCCStateManager) ClearAbortedStatus(txIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 从中止交易集合中移除
	delete(m.abortedTxs, txIndex)

	// 更新交易状态
	delete(m.txStatus, txIndex)

	// 清除读依赖关系
	delete(m.readDependencies, txIndex)

	log.Debug("清除交易中止状态",
		"txIndex", txIndex)
}
