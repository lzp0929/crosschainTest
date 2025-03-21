package mvcc

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// MVCCStateDB 是对StateDB的MVCC包装
type MVCCStateDB struct {
	baseState   *state.StateDB    // 底层状态数据库
	mvccManager *MVCCStateManager // MVCC管理器
	txIndex     int               // 当前交易在区块内的索引

	// 读写集跟踪
	readSet         map[common.Address]map[string]struct{}
	writeSet        map[common.Address]map[string]struct{}
	storageReadSet  map[common.Address]map[common.Hash]struct{}
	storageWriteSet map[common.Address]map[common.Hash]struct{}
}

// NewMVCCStateDB 创建一个新的MVCC状态包装器
func NewMVCCStateDB(baseState *state.StateDB, txIndex int) *MVCCStateDB {
	return &MVCCStateDB{
		mvccManager:     NewMVCCStateManager(0), // 使用0作为区块号
		baseState:       baseState,
		txIndex:         txIndex,
		readSet:         make(map[common.Address]map[string]struct{}),
		writeSet:        make(map[common.Address]map[string]struct{}),
		storageReadSet:  make(map[common.Address]map[common.Hash]struct{}),
		storageWriteSet: make(map[common.Address]map[common.Hash]struct{}),
	}
}

// 重写各种状态操作方法，使用MVCC处理

// GetBalance 获取账户余额
func (s *MVCCStateDB) GetBalance(addr common.Address) *big.Int {
	// 在每个读操作前检查是否需要重试
	if s.CheckAndRetry() {
		log.Debug("交易需要重试，中断当前操作", "txIndex", s.txIndex)
		return big.NewInt(0)
	}

	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "balance", s.baseState)
	if !success {
		// 如果读取失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易读取balance失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
		return big.NewInt(0)
	}
	return value.(*big.Int)
}

// SetBalance 设置账户余额
func (s *MVCCStateDB) SetBalance(addr common.Address, amount *big.Int) {
	// 在每个写操作前检查是否需要重试
	if s.CheckAndRetry() {
		log.Debug("交易需要重试，中断当前操作", "txIndex", s.txIndex)
		return
	}

	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "balance", amount)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易写入balance失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
	}
}

// GetNonce 获取账户Nonce
func (s *MVCCStateDB) GetNonce(addr common.Address) uint64 {
	// 在每个读操作前检查是否需要重试
	if s.CheckAndRetry() {
		log.Debug("交易需要重试，中断当前操作", "txIndex", s.txIndex)
		return 0
	}

	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "nonce", s.baseState)
	if !success {
		// 如果读取失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易读取nonce失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
		return 0
	}
	return value.(uint64)
}

// SetNonce 设置账户Nonce
func (s *MVCCStateDB) SetNonce(addr common.Address, nonce uint64) {
	// 在每个写操作前检查是否需要重试
	if s.CheckAndRetry() {
		log.Debug("交易需要重试，中断当前操作", "txIndex", s.txIndex)
		return
	}

	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "nonce", nonce)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易写入nonce失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
	}
}

// GetState 获取合约存储
func (s *MVCCStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	// 在每个读操作前检查是否需要重试
	if s.CheckAndRetry() {
		log.Debug("交易需要重试，中断当前操作", "txIndex", s.txIndex)
		return common.Hash{}
	}

	value, success := s.mvccManager.ReadStorageState(s.txIndex, addr, key, s.baseState)
	if !success {
		// 如果读取失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易读取storage失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex(),
				"key", key.Hex())
		}
		return common.Hash{}
	}
	return value.(common.Hash)
}

// SetState 设置合约存储
func (s *MVCCStateDB) SetState(addr common.Address, key common.Hash, value common.Hash) {
	// 在每个写操作前检查是否需要重试
	if s.CheckAndRetry() {
		log.Debug("交易需要重试，中断当前操作", "txIndex", s.txIndex)
		return
	}

	success := s.mvccManager.WriteStorageState(s.txIndex, addr, key, value)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易写入storage失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex(),
				"key", key.Hex())
		}
	}
}

// 跟踪读操作
func (s *MVCCStateDB) trackRead(addr common.Address, field string) {
	if s.readSet[addr] == nil {
		s.readSet[addr] = make(map[string]struct{})
	}
	s.readSet[addr][field] = struct{}{}
}

// 跟踪写操作
func (s *MVCCStateDB) trackWrite(addr common.Address, field string) {
	if s.writeSet[addr] == nil {
		s.writeSet[addr] = make(map[string]struct{})
	}
	s.writeSet[addr][field] = struct{}{}
}

// 跟踪存储读操作
func (s *MVCCStateDB) trackStorageRead(addr common.Address, key common.Hash) {
	if s.storageReadSet[addr] == nil {
		s.storageReadSet[addr] = make(map[common.Hash]struct{})
	}
	s.storageReadSet[addr][key] = struct{}{}
}

// 跟踪存储写操作
func (s *MVCCStateDB) trackStorageWrite(addr common.Address, key common.Hash) {
	if s.storageWriteSet[addr] == nil {
		s.storageWriteSet[addr] = make(map[common.Hash]struct{})
	}
	s.storageWriteSet[addr][key] = struct{}{}
}

// Prepare implements StateDB interface
func (s *MVCCStateDB) Prepare(hash common.Hash, index int) {
	s.baseState.Prepare(hash, index)
}

// 其他必要的StateDB方法都需要类似改造...
// 包括GetCode, SetCode, AddBalance, SubBalance等

// AddAddressToAccessList adds the given address to the access list
func (s *MVCCStateDB) AddAddressToAccessList(addr common.Address) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.AddAddressToAccessList(addr)
}

// AddSlotToAccessList adds the given (address, slot) to the access list
func (s *MVCCStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.AddSlotToAccessList(addr, slot)
}

// SlotInAccessList returns true if the given (address, slot) is in the access list
func (s *MVCCStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return false, false
	}
	return s.baseState.SlotInAccessList(addr, slot)
}

// AddressInAccessList returns true if the given address is in the access list
func (s *MVCCStateDB) AddressInAccessList(addr common.Address) bool {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return false
	}
	return s.baseState.AddressInAccessList(addr)
}

// AddBalance adds amount to the account associated with addr
func (s *MVCCStateDB) AddBalance(addr common.Address, amount *big.Int) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}

	s.trackWrite(addr, "balance")
	balance := s.GetBalance(addr)
	newBalance := new(big.Int).Add(balance, amount)
	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "balance", newBalance)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易增加balance失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
	}
}

// SubBalance subtracts amount from the account associated with addr
func (s *MVCCStateDB) SubBalance(addr common.Address, amount *big.Int) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}

	s.trackWrite(addr, "balance")
	balance := s.GetBalance(addr)
	newBalance := new(big.Int).Sub(balance, amount)
	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "balance", newBalance)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易减少balance失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
	}
}

// AddLog implements StateDB interface
func (s *MVCCStateDB) AddLog(log *types.Log) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.AddLog(log)
}

// AddPreimage records a SHA3 preimage seen by the VM
func (s *MVCCStateDB) AddPreimage(hash common.Hash, preimage []byte) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.AddPreimage(hash, preimage)
}

// AddRefund adds gas to the refund counter
func (s *MVCCStateDB) AddRefund(gas uint64) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.AddRefund(gas)
}

// SubRefund removes gas from the refund counter
func (s *MVCCStateDB) SubRefund(gas uint64) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.SubRefund(gas)
}

// GetRefund returns the current value of the refund counter
func (s *MVCCStateDB) GetRefund() uint64 {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return 0
	}
	return s.baseState.GetRefund()
}

// GetBaseState returns the underlying state.StateDB
func (s *MVCCStateDB) GetBaseState() *state.StateDB {
	return s.baseState
}

// CreateAccount creates a new account
func (s *MVCCStateDB) CreateAccount(addr common.Address) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.trackWrite(addr, "account")
	s.baseState.CreateAccount(addr)
}

// Empty returns whether the account is considered empty
func (s *MVCCStateDB) Empty(addr common.Address) bool {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return true
	}
	return s.baseState.Empty(addr)
}

// Exist reports whether the given account exists in state.
// Notably this should also return true for suicided accounts.
func (s *MVCCStateDB) Exist(addr common.Address) bool {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return false
	}
	return s.baseState.Exist(addr)
}

// ForEachStorage iterates over each storage item and calls the provided callback
func (s *MVCCStateDB) ForEachStorage(addr common.Address, cb func(key, value common.Hash) bool) error {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return nil
	}
	return s.baseState.ForEachStorage(addr, cb)
}

// GetCode returns the contract code associated with this object
func (s *MVCCStateDB) GetCode(addr common.Address) []byte {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return nil
	}
	s.trackRead(addr, "code")
	hash := s.GetCodeHash(addr)
	if hash == (common.Hash{}) {
		return nil
	}
	return s.baseState.GetCode(addr)
}

// GetCodeSize returns the size of the contract code
func (s *MVCCStateDB) GetCodeSize(addr common.Address) int {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return 0
	}
	return len(s.GetCode(addr))
}

// GetCodeHash returns the code hash
func (s *MVCCStateDB) GetCodeHash(addr common.Address) common.Hash {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return common.Hash{}
	}

	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "codehash", s.baseState)
	if !success {
		// 如果读取失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易读取codehash失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
		return common.Hash{}
	}
	return value.(common.Hash)
}

// SetCode sets the contract code
func (s *MVCCStateDB) SetCode(addr common.Address, code []byte) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.trackWrite(addr, "code")
	s.baseState.SetCode(addr, code)
}

// GetCommittedState returns the committed state of the given account
func (s *MVCCStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return common.Hash{}
	}
	return s.baseState.GetCommittedState(addr, hash)
}

// HasSuicided returns whether the given account has been suicided
func (s *MVCCStateDB) HasSuicided(addr common.Address) bool {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return false
	}
	return s.GetSuicided(addr)
}

// Suicide marks the given account as suicided
func (s *MVCCStateDB) Suicide(addr common.Address) bool {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return false
	}

	s.trackWrite(addr, "suicided")
	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "suicided", true)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易设置suicide失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
		return false
	}
	return true
}

// PrepareAccessList handles the preparatory steps for executing a state transition with
// regards to both EIP-2929 and EIP-2930
func (s *MVCCStateDB) PrepareAccessList(sender common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.PrepareAccessList(sender, dest, precompiles, txAccesses)
}

// Snapshot returns an identifier for the current revision of the state
func (s *MVCCStateDB) Snapshot() int {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return 0
	}
	return s.baseState.Snapshot()
}

// RevertToSnapshot reverts all state changes made since the given revision
func (s *MVCCStateDB) RevertToSnapshot(id int) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}
	s.baseState.RevertToSnapshot(id)
}

// 在测试代码中，修改CustomStateDB的实现
type CustomStateDB struct {
	StateDB   *state.StateDB
	mvccState *MVCCStateDB
	txIndex   int
}

// GetState 重写GetState方法，使用MVCC读取
func (db *CustomStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	// 记录读操作
	db.mvccState.trackStorageRead(addr, key)

	// 从MVCC管理器读取
	value, success := db.mvccState.mvccManager.ReadStorageState(db.txIndex, addr, key, db.StateDB)
	if !success {
		// 读取失败，返回空值
		return common.Hash{}
	}

	if value == nil {
		return common.Hash{}
	}

	return value.(common.Hash)
}

// SetState 重写SetState方法，使用MVCC写入
func (db *CustomStateDB) SetState(addr common.Address, key common.Hash, value common.Hash) {
	// 记录写操作
	db.mvccState.trackStorageWrite(addr, key)

	// 写入MVCC管理器
	success := db.mvccState.mvccManager.WriteStorageState(db.txIndex, addr, key, value)
	if !success {
		// 如果写入失败，检查是否为WAR冲突
		if db.mvccState.mvccManager.ShouldRerun(db.txIndex) {
			log.Debug("交易执行冲突，需要重试",
				"txIndex", db.txIndex,
				"addr", addr.Hex(),
				"key", key.Hex())
		}
	}
}

// ReadStorageState 读取存储状态，支持MVCC
func (w *MVCCStateDB) ReadStorageState(addr common.Address, key common.Hash, txIndex int) common.Hash {
	// 从MVCC管理器读取
	value, success := w.mvccManager.ReadStorageState(txIndex, addr, key, w.baseState)
	if !success {
		// 如果读取失败，检查是否为WAR冲突
		if w.mvccManager.ShouldRerun(txIndex) {
			log.Debug("交易读取storage失败，需要重试",
				"txIndex", txIndex,
				"addr", addr.Hex(),
				"key", key.Hex())

			// 获取需要重试的键
			rerunKeys := w.mvccManager.GetRerunKeys(txIndex)
			log.Debug("需要重试的键",
				"txIndex", txIndex,
				"keys", rerunKeys)
		}

		// 读取失败，返回空值
		return common.Hash{}
	}

	// 检查是否是nil值
	if value == nil {
		log.Debug("读取到nil值",
			"txIndex", txIndex,
			"address", addr,
			"key", key)

		// 返回特殊值表示需要等待
		return common.Hash{0xff} // 特殊标记，表示需要等待
	}

	return value.(common.Hash)
}

func (s *MVCCStateDB) GetSuicided(addr common.Address) bool {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return false
	}

	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "suicided", s.baseState)
	if !success {
		// 如果读取失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易读取suicided失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
		return false
	}
	return value.(bool)
}

func (s *MVCCStateDB) SetSuicided(addr common.Address, suicided bool) {
	// 检查是否需要重试
	if s.CheckAndRetry() {
		return
	}

	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "suicided", suicided)
	if !success {
		// 如果写入失败，检查是否被标记为需要重试
		if s.mvccManager.ShouldRerun(s.txIndex) {
			log.Debug("交易设置suicided失败，需要重试",
				"txIndex", s.txIndex,
				"addr", addr.Hex())
		}
	}
}

func (s *MVCCStateDB) GetMVCCManager() *MVCCStateManager {
	return s.mvccManager
}

// IsTransactionAborted 检查交易是否已中止
func (s *MVCCStateDB) IsTransactionAborted() bool {
	return s.mvccManager.IsTransactionAborted(s.txIndex)
}

// ShouldRerun 检查交易是否需要重新执行
func (s *MVCCStateDB) ShouldRerun() bool {
	return s.mvccManager.ShouldRerun(s.txIndex)
}

// GetRerunKeys 获取需要重新执行的键
func (s *MVCCStateDB) GetRerunKeys() []string {
	return s.mvccManager.GetRerunKeys(s.txIndex)
}

// CheckAndRetry 检查交易是否需要重试并处理重试逻辑
func (s *MVCCStateDB) CheckAndRetry() bool {
	// 检查交易是否被标记为需要重试
	if !s.mvccManager.ShouldRerun(s.txIndex) {
		return false
	}

	// 获取当前重试次数
	retryCount := s.mvccManager.GetRetryCount(s.txIndex)

	log.Debug("检测到交易需要重试",
		"txIndex", s.txIndex,
		"retryCount", retryCount)

	// 清除交易状态，准备重试
	s.mvccManager.ClearTransactionState(s.txIndex)

	// 返回true表示当前操作需要中断，交易需要重试
	return true
}

// RetryTransaction 尝试重新执行交易，返回是否成功重试
func (s *MVCCStateDB) RetryTransaction() bool {
	return s.mvccManager.RetryTransaction(s.txIndex)
}

// GetRetryCount 获取当前重试次数
func (s *MVCCStateDB) GetRetryCount() int {
	return s.mvccManager.GetRetryCount(s.txIndex)
}

// GetTxIndex 获取当前交易索引
func (s *MVCCStateDB) GetTxIndex() int {
	return s.txIndex
}

// GetLogs 获取交易日志
func (s *MVCCStateDB) GetLogs() []*types.Log {
	return s.baseState.GetLogs(common.Hash{}, common.Hash{})
}
