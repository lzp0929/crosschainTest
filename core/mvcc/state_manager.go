package mvcc

import (
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

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

// Transaction 表示一个交易
type Transaction struct {
	ID         int                 // 交易ID
	Status     string              // 交易状态: "executing", "completed", "finalized"
	RerunKeys  map[string]struct{} // 需要重试的键列表
	BerunFlag  bool                // 重试标志
	Retries    int                 // 重试次数
	MaxRetries int                 // 最大重试次数
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

	// 交易执行状态表: 交易索引 -> 交易状态
	transactions map[int]*Transaction

	// 等待依赖关系: 等待的交易索引 -> 依赖的交易索引
	waitForTx map[int]int

	// 调试模式
	debug bool
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
		transactions:     make(map[int]*Transaction),
		waitForTx:        make(map[int]int),
	}
}

// ReadStorageState 读取存储状态
func (m *MVCCStateManager) ReadStorageState(txIndex int, addr common.Address, key common.Hash, baseState *state.StateDB) (interface{}, bool) {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		log.Debug(fmt.Sprintf("交易%d: 已中止，读取失败", txIndex))
		return nil, false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 尝试从多版本状态表中读取
	var latestVersion *VersionedValue
	var maxTxIndex = -1

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
				log.Debug(fmt.Sprintf("交易%d: 读取到已中止的版本，跳过 (writer交易%d)",
					txIndex, latestVersion.TxIndex))
				return nil, false
			}

			// 记录读者
			if latestVersion.Readers == nil {
				latestVersion.Readers = make(map[int]struct{})
			}
			latestVersion.Readers[txIndex] = struct{}{}

			log.Debug(fmt.Sprintf("交易%d: 成功读取到交易%d写入的版本，值为: %v",
				txIndex, latestVersion.TxIndex, latestVersion.Value))

			return latestVersion.Value, true
		}
	}

	// 如果没有找到合适的版本，从基础状态读取
	baseValue := baseState.GetState(addr, key)

	log.Debug(fmt.Sprintf("交易%d: 从基础状态读取键 %s，值为: %v",
		txIndex, key.Hex(), baseValue))

	// 记录读者到最早的版本
	if m.storageStates[addr] == nil {
		m.storageStates[addr] = make(map[common.Hash][]VersionedValue)
	}
	if m.storageStates[addr][key] == nil {
		m.storageStates[addr][key] = []VersionedValue{}
	}

	// 如果没有任何版本，创建一个初始版本来记录读者
	if len(m.storageStates[addr][key]) == 0 {
		initialVersion := VersionedValue{
			TxIndex:   -1, // 使用-1表示初始状态
			Value:     baseValue,
			IsAborted: false,
			Readers:   map[int]struct{}{txIndex: {}},
		}
		m.storageStates[addr][key] = append(m.storageStates[addr][key], initialVersion)
	} else {
		// 将读者添加到最早的版本
		m.storageStates[addr][key][0].Readers[txIndex] = struct{}{}
	}

	return baseValue, true
}

// WriteStorageState 写入存储状态
func (m *MVCCStateManager) WriteStorageState(txIndex int, addr common.Address, key common.Hash, value interface{}) bool {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		log.Debug(fmt.Sprintf("交易%d: 已中止，写入失败", txIndex))
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

	versions := m.storageStates[addr][key]
	// 检查所有id号小于自身的版本
	for _, version := range versions {
		if version.TxIndex < txIndex {
			// 检查是否有id号更大的交易读取了这个版本
			for readerTx := range version.Readers {
				if readerTx > txIndex {
					// 标记WAR冲突
					if tx := m.transactions[readerTx]; tx != nil {
						tx.BerunFlag = true
						if tx.RerunKeys == nil {
							tx.RerunKeys = make(map[string]struct{})
						}
						tx.RerunKeys[key.String()] = struct{}{}
						// 记录等待关系：被标记的交易需要等待当前交易
						m.waitForTx[readerTx] = txIndex

						// 如果高ID交易已经标记为finalized或completed，立即降级为executing
						if tx.Status == "finalized" || tx.Status == "completed" {
							tx.Status = "executing"
							log.Debug(fmt.Sprintf("交易%d: 发现已完成的高ID交易%d存在WAR冲突，将其状态降级为executing",
								txIndex, readerTx))
						}

						log.Debug(fmt.Sprintf("交易%d: 检测到WAR冲突 - 高ID交易%d读取了将被写入的版本(读取的是交易%d的版本，值为%v)",
							txIndex, readerTx, version.TxIndex, version.Value))
					}
				}
			}
		}
	}

	// 强化WAR冲突检测: 检查是否修改正在被读取的数据
	var conflictingReaders []int
	for _, version := range versions {
		for readerTx := range version.Readers {
			// 如果是高序号交易在读,而当前交易要写,存在WAR冲突
			if readerTx > txIndex {
				conflictingReaders = append(conflictingReaders, readerTx)
				// 标记WAR冲突
				if tx := m.transactions[readerTx]; tx != nil {
					tx.BerunFlag = true
					if tx.RerunKeys == nil {
						tx.RerunKeys = make(map[string]struct{})
					}
					tx.RerunKeys[key.String()] = struct{}{}
					// 记录等待关系
					m.waitForTx[readerTx] = txIndex

					// 如果高ID交易已经标记为finalized或completed，立即降级为executing
					if tx.Status == "finalized" || tx.Status == "completed" {
						tx.Status = "executing"
						log.Debug(fmt.Sprintf("交易%d: 发现已完成的高ID交易%d存在强化WAR冲突，将其状态降级为executing",
							txIndex, readerTx))
					}

					log.Debug(fmt.Sprintf("交易%d: 检测到强化WAR冲突 - 高ID交易%d正在读取将被修改的数据(当前值为%v)",
						txIndex, readerTx, version.Value))
				}
			}
		}
	}

	// 创建新版本
	newVersion := VersionedValue{
		TxIndex:   txIndex,
		Value:     value,
		IsAborted: false,
		Readers:   make(map[int]struct{}),
	}

	// 添加新版本
	m.storageStates[addr][key] = append(m.storageStates[addr][key], newVersion)

	log.Debug(fmt.Sprintf("交易%d: 写入新版本 - 键: %s, 值: %v",
		txIndex, key.Hex(), value))

	if len(conflictingReaders) > 0 {
		log.Debug(fmt.Sprintf("交易%d: 写入操作影响到的读取交易: %v", txIndex, conflictingReaders))
	}

	return true
}

// ReadAddressState 读取地址状态
func (m *MVCCStateManager) ReadAddressState(txIndex int, addr common.Address, field string, baseState *state.StateDB) (interface{}, bool) {
	// 检查交易是否已中止
	m.mu.Lock()
	isAborted := m.IsTransactionAborted(txIndex)
	m.mu.Unlock()

	if isAborted {
		log.Debug(fmt.Sprintf("交易%d: 已中止，读取状态失败", txIndex))
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
				log.Debug(fmt.Sprintf("交易%d: 读取到已中止的版本，跳过(writer交易%d)",
					txIndex, latestVersion.TxIndex))
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

			log.Debug(fmt.Sprintf("交易%d: 成功读取地址状态 - 字段=%s, 写入方交易=%d, 值=%v",
				txIndex, field, latestVersion.TxIndex, latestVersion.Value))
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

	log.Debug(fmt.Sprintf("交易%d: 从基础状态读取地址状态 - 地址=%s, 字段=%s, 值=%v",
		txIndex, addr.Hex(), field, baseValue))

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
		log.Debug(fmt.Sprintf("交易%d: 已中止，写入地址状态失败", txIndex))
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
					log.Debug(fmt.Sprintf("交易%d: 标记高ID交易%d中止，因为它读取了旧版本(依赖交易=%d)",
						txIndex, readerTx, depTx))
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
					log.Debug(fmt.Sprintf("交易%d: 标记高ID交易%d中止，因为它读取了将被覆盖的版本(版本交易=%d)",
						txIndex, readerTx, version.TxIndex))
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
			log.Debug(fmt.Sprintf("交易%d: 当前交易在中止其他交易过程中被中止", txIndex))
			return false
		}
	}

	// 创建或更新版本
	if existingVersionIndex >= 0 {
		// 更新已存在的版本
		existingVersions[existingVersionIndex].Value = value
		existingVersions[existingVersionIndex].IsAborted = false

		log.Debug(fmt.Sprintf("交易%d: 更新交易的已存在地址状态 - 地址=%s, 字段=%s, 值=%v",
			txIndex, addr.Hex(), field, value))
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

		log.Debug(fmt.Sprintf("交易%d: 写入新地址状态 - 地址=%s, 字段=%s, 值=%v",
			txIndex, addr.Hex(), field, value))
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
					log.Debug(fmt.Sprintf("交易%d: 标记地址状态版本为已中止并清除值 - 地址=%s, 字段=%s",
						txIndex, addr.Hex(), field))
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
					log.Debug(fmt.Sprintf("交易%d: 标记存储状态版本为已中止并清除值 - 地址=%s, 键=%s",
						txIndex, addr.Hex(), key.Hex()))
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

	log.Debug(fmt.Sprintf("交易%d: 已中止", txIndex))
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

// ExecuteTransaction 执行交易，并处理交易的状态更新
func (m *MVCCStateManager) ExecuteTransaction(txIndex int, executeFn func() error) error {
	m.mu.Lock()
	if _, exists := m.transactions[txIndex]; !exists {
		m.transactions[txIndex] = &Transaction{
			ID:         txIndex,
			Status:     "executing",
			RerunKeys:  make(map[string]struct{}),
			BerunFlag:  false,
			Retries:    0,
			MaxRetries: 10,
		}
		log.Debug(fmt.Sprintf("交易%d: 创建新交易,初始状态为executing", txIndex))
	}
	m.mu.Unlock()

	// 处理需要重新执行的情况，可能是由于WAR冲突引起的
	for {
		m.mu.Lock()
		tx := m.transactions[txIndex]
		if tx.BerunFlag {
			log.Debug(fmt.Sprintf("交易%d: 检测到交易需要重试 BerunFlag=true，重试次数=%d，冲突键数量=%d",
				txIndex, tx.Retries, len(tx.RerunKeys)))

			// 在WAR冲突的情况下，需要等待前序交易完成
			// 获取需要等待的交易ID
			waitForID, exists := m.waitForTx[txIndex]
			if exists {
				waitForTx := m.transactions[waitForID]
				if waitForTx != nil && waitForTx.Status != "completed" && waitForTx.Status != "finalized" {
					log.Debug(fmt.Sprintf("交易%d: 等待依赖交易%d完成后再重试，当前状态: %s",
						txIndex, waitForID, waitForTx.Status))
					// 设置最大等待时间，避免永久等待
					maxWaitTime := 30 * time.Second
					waitStart := time.Now()

					m.mu.Unlock()

					// 使用指数退避算法等待
					waitTime := 1 * time.Millisecond
					maxWaitStep := 100 * time.Millisecond

					for {
						if time.Since(waitStart) > maxWaitTime {
							log.Debug(fmt.Sprintf("交易%d: 等待依赖交易%d超时", txIndex, waitForID))
							return fmt.Errorf("等待依赖交易超时")
						}

						m.mu.Lock()
						waitForTx = m.transactions[waitForID]
						if waitForTx == nil || waitForTx.Status == "completed" || waitForTx.Status == "finalized" {
							log.Debug(fmt.Sprintf("交易%d: 依赖交易%d已完成(%s)，可以继续",
								txIndex, waitForID, waitForTx.Status))
							m.mu.Unlock()
							break
						}
						m.mu.Unlock()

						time.Sleep(waitTime)
						waitTime = waitTime * 2
						if waitTime > maxWaitStep {
							waitTime = maxWaitStep
						}
					}

					m.mu.Lock()
				}
			}

			// 重置标志，准备重新执行
			tx.BerunFlag = false
			tx.Retries++
			log.Debug(fmt.Sprintf("交易%d: 重置BerunFlag准备第%d次重试(maxRetries=%d)",
				txIndex, tx.Retries, tx.MaxRetries))

			if tx.Retries > tx.MaxRetries {
				log.Debug(fmt.Sprintf("交易%d: 交易重试次数超过上限,放弃执行(retries=%d)",
					txIndex, tx.Retries))
				m.mu.Unlock()
				return fmt.Errorf("交易重试次数超过上限: %d/%d", tx.Retries, tx.MaxRetries)
			}
			delete(m.waitForTx, txIndex)
		}
		m.mu.Unlock()

		// 执行交易逻辑
		err := executeFn()
		if err != nil {
			log.Debug(fmt.Sprintf("交易%d: 交易执行失败, error=%v", txIndex, err))
			return err
		}

		// 检查是否需要重新执行
		m.mu.Lock()
		tx = m.transactions[txIndex]
		if tx.BerunFlag {
			// 如果在执行过程中标记了需要重新执行，则继续循环
			log.Debug(fmt.Sprintf("交易%d: 执行后检测到BerunFlag=true,需要重试", txIndex))
			m.mu.Unlock()
			continue
		}

		// 标记交易为completed
		tx.Status = "completed"
		log.Debug(fmt.Sprintf("交易%d: 交易执行完成,状态变更为completed", txIndex))

		// 检查是否在WAR冲突后执行，如果是则需要等待前序交易完成
		needWaitPrevious := true // 总是等待前序交易，确保低ID交易先完成
		m.mu.Unlock()

		// 如果需要等待前序交易，则调用等待函数
		if needWaitPrevious {
			log.Debug(fmt.Sprintf("交易%d: 需要等待前序交易", txIndex))
			log.Debug(fmt.Sprintf("交易%d: 等待前序交易finalized中", txIndex))
			if !m.waitForPreviousTxsFinalized(txIndex) {
				return fmt.Errorf("等待前序交易超时")
			}
		}

		// 标记当前交易为finalized
		m.mu.Lock()
		tx = m.transactions[txIndex]
		if tx != nil {
			// 再次检查BerunFlag，确保在finalized前没有新的冲突
			if tx.BerunFlag {
				log.Debug(fmt.Sprintf("交易%d: 即将标记为finalized时发现BerunFlag=true，需要重试", txIndex))
				m.mu.Unlock()
				continue
			}

			// 最后一次检查所有前序交易，确保它们都是finalized状态
			allPrevFinalized := true
			for i := 0; i < txIndex; i++ {
				prevTx, exists := m.transactions[i]
				if !exists || prevTx.Status != "finalized" {
					allPrevFinalized = false
					log.Debug(fmt.Sprintf("交易%d: 在最终检查中发现前序交易%d未finalized", txIndex, i))
					break
				}
			}

			if !allPrevFinalized {
				// 如果前序交易还未完成，先释放锁，等待一会再重试
				m.mu.Unlock()
				log.Debug(fmt.Sprintf("交易%d: 最终检查发现有前序交易未完成，等待100ms再重试", txIndex))
				time.Sleep(100 * time.Millisecond)
				continue
			}

			tx.Status = "finalized"
			log.Debug(fmt.Sprintf("交易%d: 所有前序交易已完成，标记当前交易为finalized", txIndex))
		}
		m.mu.Unlock()

		break
	}

	return nil
}

// ShouldRerun 检查交易是否需要重新执行
func (m *MVCCStateManager) ShouldRerun(txIndex int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactions[txIndex]
	if !exists {
		return false
	}

	return tx.BerunFlag && tx.Retries < tx.MaxRetries
}

// GetRerunKeys 获取需要重新执行的键
func (m *MVCCStateManager) GetRerunKeys(txIndex int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactions[txIndex]
	if !exists {
		return nil
	}

	keys := make([]string, 0, len(tx.RerunKeys))
	for key := range tx.RerunKeys {
		keys = append(keys, key)
	}
	return keys
}

// GetRetryCount 获取当前重试次数
func (m *MVCCStateManager) GetRetryCount(txIndex int) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactions[txIndex]
	if !exists {
		return 0
	}

	return tx.Retries
}

// ClearTransactionState 清除交易状态，准备重试
func (m *MVCCStateManager) ClearTransactionState(txIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Debug("准备清除交易状态用于重试",
		"txIndex", txIndex,
		"currentRetries", m.transactions[txIndex].Retries)

	// 清除交易的读写依赖
	delete(m.readDependencies, txIndex)

	// 清除交易在存储状态表中的版本
	versionsRemoved := 0
	for addr, keys := range m.storageStates {
		for key, versions := range keys {
			newVersions := make([]VersionedValue, 0, len(versions))
			for _, version := range versions {
				if version.TxIndex != txIndex {
					newVersions = append(newVersions, version)
				} else {
					versionsRemoved++
				}
			}
			m.storageStates[addr][key] = newVersions
		}
	}

	// 清除交易在地址状态表中的版本
	for addr, fields := range m.addressStates {
		for field, versions := range fields {
			newVersions := make([]VersionedValue, 0, len(versions))
			for _, version := range versions {
				if version.TxIndex != txIndex {
					newVersions = append(newVersions, version)
				} else {
					versionsRemoved++
				}
			}
			m.addressStates[addr][field] = newVersions
		}
	}

	// 重置交易状态
	if tx, exists := m.transactions[txIndex]; exists {
		oldStatus := tx.Status
		tx.Status = "executing"
		tx.BerunFlag = false
		rerunKeys := make([]string, 0, len(tx.RerunKeys))
		for k := range tx.RerunKeys {
			rerunKeys = append(rerunKeys, k)
		}
		tx.RerunKeys = make(map[string]struct{})
		log.Debug("清除交易状态完成,准备重试",
			"txIndex", txIndex,
			"oldStatus", oldStatus,
			"newStatus", "executing",
			"oldRetries", tx.Retries-1,
			"newRetries", tx.Retries,
			"versionsRemoved", versionsRemoved,
			"previousRerunKeys", rerunKeys)
	}
}

// RetryTransaction 尝试重新执行交易
func (m *MVCCStateManager) RetryTransaction(txIndex int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactions[txIndex]
	if !exists {
		return false
	}

	// 检查重试次数是否超过限制
	if tx.Retries >= tx.MaxRetries {
		log.Debug("交易重试次数超过限制，放弃重试",
			"txIndex", txIndex,
			"retries", tx.Retries,
			"maxRetries", tx.MaxRetries)
		return false
	}

	// 标记交易需要重试
	tx.BerunFlag = true
	log.Debug("标记交易需要重试",
		"txIndex", txIndex,
		"retries", tx.Retries)

	return true
}

// waitForPreviousTxsFinalized 等待所有较低ID的交易完成
func (m *MVCCStateManager) waitForPreviousTxsFinalized(txIdx int) bool {
	if txIdx == 0 {
		// 第一笔交易不需要等待
		log.Debug(fmt.Sprintf("交易%d: 第一笔交易直接标记为finalized", txIdx))
		return true
	}

	log.Debug(fmt.Sprintf("交易%d: 开始等待前序交易finalized", txIdx))
	startWaitTime := time.Now()
	maxWaitTime := 60 * time.Second

	// 使用指数退避算法
	waitTime := 1 * time.Millisecond
	maxWaitStep := 100 * time.Millisecond

	for {
		if time.Since(startWaitTime) > maxWaitTime {
			log.Debug(fmt.Sprintf("交易%d: 等待前序交易超时", txIdx))
			return false
		}

		m.mu.Lock()
		// 严格按ID顺序等待所有前序交易
		pendingTxs := []int{}
		for i := 0; i < txIdx; i++ {
			tx, exists := m.transactions[i]
			if !exists {
				// 为缺失的交易创建记录并标记为已完成
				m.transactions[i] = &Transaction{
					ID:     i,
					Status: "finalized",
				}
				log.Debug(fmt.Sprintf("交易%d: 为缺失的前序交易%d创建记录并标记为finalized", txIdx, i))
			} else if tx.Status != "finalized" {
				// 添加到待处理列表，必须等待前序交易达到finalized状态（不接受completed状态）
				pendingTxs = append(pendingTxs, i)
				log.Debug(fmt.Sprintf("交易%d: 等待前序交易%d(%s)完成", txIdx, i, tx.Status))
			}
		}
		m.mu.Unlock()

		if len(pendingTxs) == 0 {
			log.Debug(fmt.Sprintf("交易%d: 所有前序交易已完成，可以标记为finalized", txIdx))
			return true
		}

		log.Debug(fmt.Sprintf("交易%d: 仍有%d个前序交易未完成: %v", txIdx, len(pendingTxs), pendingTxs))

		// 指数退避
		time.Sleep(waitTime)
		waitTime = waitTime * 2
		if waitTime > maxWaitStep {
			waitTime = maxWaitStep
		}
	}
}

// CommitAllToState 将所有最终状态提交到基础状态数据库
func (m *MVCCStateManager) CommitAllToState(stateDB *state.StateDB) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 按照交易ID顺序提交
	var txIDs []int
	for txID := range m.transactions {
		txIDs = append(txIDs, txID)
	}
	sort.Ints(txIDs)

	log.Debug("开始提交MVCC状态到基础状态数据库", "交易数量", len(txIDs))

	// 提交所有地址状态
	for addr, fields := range m.addressStates {
		for field, versions := range fields {
			// 找到最新的非中止版本
			var latestValue interface{}
			var latestTxID int = -1

			for i := len(versions) - 1; i >= 0; i-- {
				if !versions[i].IsAborted && versions[i].TxIndex > latestTxID {
					latestValue = versions[i].Value
					latestTxID = versions[i].TxIndex
					break
				}
			}

			// 应用到状态
			if latestValue != nil {
				switch field {
				case "balance":
					stateDB.SetBalance(addr, latestValue.(*big.Int))
					log.Debug("提交余额状态", "地址", addr.Hex(), "余额", latestValue.(*big.Int))
				case "nonce":
					stateDB.SetNonce(addr, latestValue.(uint64))
					log.Debug("提交Nonce状态", "地址", addr.Hex(), "nonce", latestValue.(uint64))
				case "code":
					stateDB.SetCode(addr, latestValue.([]byte))
					log.Debug("提交Code状态", "地址", addr.Hex(), "codeSize", len(latestValue.([]byte)))
				case "codehash":
					// codehash不需要直接设置，SetCode会更新它
					log.Debug("跳过Codehash状态", "地址", addr.Hex())
				case "suicided":
					if latestValue.(bool) {
						stateDB.Suicide(addr)
						log.Debug("提交自毁状态", "地址", addr.Hex())
					}
				}
			}
		}
	}

	// 提交所有存储状态
	for addr, keys := range m.storageStates {
		for key, versions := range keys {
			// 找到最新的非中止版本
			var latestValue interface{}
			var latestTxID int = -1

			for i := len(versions) - 1; i >= 0; i-- {
				if !versions[i].IsAborted && versions[i].TxIndex > latestTxID {
					latestValue = versions[i].Value
					latestTxID = versions[i].TxIndex
					break
				}
			}

			// 应用到状态
			if latestValue != nil {
				stateDB.SetState(addr, key, latestValue.(common.Hash))
				log.Debug("提交存储状态", "地址", addr.Hex(), "键", key.Hex(), "值", latestValue.(common.Hash).Hex())
			}
		}
	}

	log.Debug("MVCC状态提交完成")
}
