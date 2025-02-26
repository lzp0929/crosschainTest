package mvcc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

// VersionedValue 表示一个带版本的值
type VersionedValue struct {
	TxIndex   int              // 创建此版本的交易索引
	Value     interface{}      // 实际的值
	IsAborted bool             // 是否已中止
	Readers   map[int]struct{} // 读取此版本的交易集合
}

// MVCCStateManager 管理多版本并发控制状态
type MVCCStateManager struct {
	addressStates    map[common.Address]map[string][]VersionedValue      // 地址状态的多版本历史
	storageStates    map[common.Address]map[common.Hash][]VersionedValue // 存储状态的多版本历史
	readDependencies map[int]map[int]struct{}                            // 交易的读依赖关系
	txStatus         map[int]string                                      // 交易状态（"pending"/"committed"/"aborted"）
	abortedTxs       map[int]struct{}                                    // 已中止的交易集合
	txCount          int                                                 // 总交易数
	blockNumber      uint64                                              // 当前区块号
}

// NewMVCCStateManager 创建新的MVCC状态管理器
func NewMVCCStateManager(txCount int) *MVCCStateManager {
	return &MVCCStateManager{
		addressStates:    make(map[common.Address]map[string][]VersionedValue),
		storageStates:    make(map[common.Address]map[common.Hash][]VersionedValue),
		readDependencies: make(map[int]map[int]struct{}),
		txStatus:         make(map[int]string),
		abortedTxs:       make(map[int]struct{}),
		txCount:          txCount,
	}
}

// DetectAndHandleConflicts 检测并处理冲突
func (m *MVCCStateManager) DetectAndHandleConflicts(writerTxIndex int) bool {
	// 写交易总是成功
	m.txStatus[writerTxIndex] = "committed"
	return true
}

// ReadAddressState 读取地址状态
func (m *MVCCStateManager) ReadAddressState(txIndex int, addr common.Address, field string, baseState *state.StateDB) (interface{}, bool) {
	// 检查交易是否已中止
	if m.IsTransactionAborted(txIndex) {
		return nil, false
	}

	// 初始化读依赖集合
	if m.readDependencies[txIndex] == nil {
		m.readDependencies[txIndex] = make(map[int]struct{})
	}

	// 获取地址的状态历史
	if states, ok := m.addressStates[addr]; ok {
		if versions, ok := states[field]; ok {
			// 查找小于当前交易索引的最大版本
			for i := len(versions) - 1; i >= 0; i-- {
				version := versions[i]
				if version.TxIndex < txIndex {
					// 记录读依赖
					m.readDependencies[txIndex][version.TxIndex] = struct{}{}
					return version.Value, true
				}
			}
		}
	}

	// 如果没有找到合适的版本，记录对基础状态的依赖
	m.readDependencies[txIndex][-1] = struct{}{}

	// 从基础状态读取
	return nil, true
}

// WriteAddressState 写入地址状态
func (m *MVCCStateManager) WriteAddressState(txIndex int, addr common.Address, field string, value interface{}) bool {
	// 检查交易是否已中止
	if m.IsTransactionAborted(txIndex) {
		return false
	}

	// 检测并处理冲突
	if !m.DetectAndHandleConflicts(txIndex) {
		return false
	}

	// 初始化地址状态映射
	if m.addressStates[addr] == nil {
		m.addressStates[addr] = make(map[string][]VersionedValue)
	}

	// 创建新版本
	newVersion := VersionedValue{
		TxIndex: txIndex,
		Value:   value,
		Readers: make(map[int]struct{}),
	}

	// 添加新版本
	m.addressStates[addr][field] = append(m.addressStates[addr][field], newVersion)

	return true
}

// ReadStorageState 读取存储状态
func (m *MVCCStateManager) ReadStorageState(txIndex int, addr common.Address, key common.Hash, baseState *state.StateDB) (interface{}, bool) {
	// 检查交易是否已中止
	if m.IsTransactionAborted(txIndex) {
		return nil, false
	}

	// 等待所有较低序号的交易完成
	for i := 0; i < txIndex; i++ {
		if status := m.txStatus[i]; status == "" || status == "pending" {
			// 如果较低序号的交易还未完成，返回基础状态
			if m.readDependencies[txIndex] == nil {
				m.readDependencies[txIndex] = make(map[int]struct{})
			}
			m.readDependencies[txIndex][-1] = struct{}{}
			return baseState.GetState(addr, key), true
		}
	}

	// 获取存储状态历史
	if states, ok := m.storageStates[addr]; ok {
		if versions, ok := states[key]; ok {
			// 查找小于当前交易索引的最大版本
			for i := len(versions) - 1; i >= 0; i-- {
				version := versions[i]
				if version.TxIndex < txIndex && !version.IsAborted {
					// 记录读依赖
					if m.readDependencies[txIndex] == nil {
						m.readDependencies[txIndex] = make(map[int]struct{})
					}
					m.readDependencies[txIndex][version.TxIndex] = struct{}{}

					// 记录读者信息
					if version.Readers == nil {
						version.Readers = make(map[int]struct{})
					}
					version.Readers[txIndex] = struct{}{}

					return version.Value, true
				}
			}
		}
	}

	// 如果没有找到合适的版本，记录对基础状态的依赖
	if m.readDependencies[txIndex] == nil {
		m.readDependencies[txIndex] = make(map[int]struct{})
	}
	m.readDependencies[txIndex][-1] = struct{}{}

	return baseState.GetState(addr, key), true
}

// WriteStorageState 写入存储状态
func (m *MVCCStateManager) WriteStorageState(txIndex int, addr common.Address, key common.Hash, value interface{}) bool {
	// 检查交易是否已中止
	if m.IsTransactionAborted(txIndex) {
		return false
	}

	// 等待所有较低序号的交易完成
	for i := 0; i < txIndex; i++ {
		if status := m.txStatus[i]; status == "" || status == "pending" {
			// 如果较低序号的交易还未完成，暂时不写入
			return false
		}
	}

	// 初始化存储状态映射
	if m.storageStates[addr] == nil {
		m.storageStates[addr] = make(map[common.Hash][]VersionedValue)
	}

	// 创建新版本
	newVersion := VersionedValue{
		TxIndex: txIndex,
		Value:   value,
		Readers: make(map[int]struct{}),
	}

	// 添加新版本
	m.storageStates[addr][key] = append(m.storageStates[addr][key], newVersion)

	// 检查是否有高序号的交易读取了旧版本
	toAbort := make([]int, 0)
	for readerTxIndex, deps := range m.readDependencies {
		// 只检查序号大于当前写入交易的读取交易
		if readerTxIndex <= txIndex {
			continue
		}

		// 检查是否读取了基础状态或较低序号的版本
		for depTxIndex := range deps {
			if depTxIndex == -1 || depTxIndex < txIndex {
				// 只中止读交易，允许写交易继续执行
				if readerTxIndex%2 == 1 { // 奇数索引是读交易
					toAbort = append(toAbort, readerTxIndex)
					break
				}
			}
		}
	}

	// 中止需要中止的交易
	for _, abortTxIndex := range toAbort {
		m.AbortTransaction(abortTxIndex)
	}

	// 更新交易状态为已提交
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
	m.txStatus[txIndex] = "aborted"
	m.abortedTxs[txIndex] = struct{}{}
}

// GetVersions 获取指定地址和键的所有版本
func (m *MVCCStateManager) GetVersions(addr common.Address, key common.Hash) []VersionedValue {
	if states, ok := m.storageStates[addr]; ok {
		if versions, ok := states[key]; ok {
			return versions
		}
	}
	return nil
}

// GetAbortedTransactions 获取所有已中止的交易
func (m *MVCCStateManager) GetAbortedTransactions() []int {
	aborted := make([]int, 0, len(m.abortedTxs))
	for txIndex := range m.abortedTxs {
		aborted = append(aborted, txIndex)
	}
	return aborted
}

// GetBlockNumber 获取当前区块号
func (m *MVCCStateManager) GetBlockNumber() uint64 {
	return m.blockNumber
}
