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
	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "balance", s.baseState)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
		return big.NewInt(0)
	}
	return value.(*big.Int)
}

// SetBalance 设置账户余额
func (s *MVCCStateDB) SetBalance(addr common.Address, amount *big.Int) {
	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "balance", amount)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
	}
}

// GetNonce 获取账户Nonce
func (s *MVCCStateDB) GetNonce(addr common.Address) uint64 {
	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "nonce", s.baseState)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
		return 0
	}
	return value.(uint64)
}

// SetNonce 设置账户Nonce
func (s *MVCCStateDB) SetNonce(addr common.Address, nonce uint64) {
	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "nonce", nonce)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
	}
}

// GetState 获取合约存储
func (s *MVCCStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	value, success := s.mvccManager.ReadStorageState(s.txIndex, addr, key, s.baseState)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
		return common.Hash{}
	}
	return value.(common.Hash)
}

// SetState 设置合约存储
func (s *MVCCStateDB) SetState(addr common.Address, key common.Hash, value common.Hash) {
	success := s.mvccManager.WriteStorageState(s.txIndex, addr, key, value)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
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
	s.baseState.AddAddressToAccessList(addr)
}

// AddSlotToAccessList adds the given (address, slot) to the access list
func (s *MVCCStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	s.baseState.AddSlotToAccessList(addr, slot)
}

// SlotInAccessList returns true if the given (address, slot) is in the access list
func (s *MVCCStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	return s.baseState.SlotInAccessList(addr, slot)
}

// AddressInAccessList returns true if the given address is in the access list
func (s *MVCCStateDB) AddressInAccessList(addr common.Address) bool {
	return s.baseState.AddressInAccessList(addr)
}

// AddBalance adds amount to the account associated with addr
func (s *MVCCStateDB) AddBalance(addr common.Address, amount *big.Int) {
	s.trackWrite(addr, "balance")
	balance := s.GetBalance(addr)
	newBalance := new(big.Int).Add(balance, amount)
	s.mvccManager.WriteAddressState(s.txIndex, addr, "balance", newBalance)
}

// SubBalance subtracts amount from the account associated with addr
func (s *MVCCStateDB) SubBalance(addr common.Address, amount *big.Int) {
	s.trackWrite(addr, "balance")
	balance := s.GetBalance(addr)
	newBalance := new(big.Int).Sub(balance, amount)
	s.mvccManager.WriteAddressState(s.txIndex, addr, "balance", newBalance)
}

// AddLog implements StateDB interface
func (s *MVCCStateDB) AddLog(log *types.Log) {
	s.baseState.AddLog(log)
}

// AddPreimage records a SHA3 preimage seen by the VM
func (s *MVCCStateDB) AddPreimage(hash common.Hash, preimage []byte) {
	s.baseState.AddPreimage(hash, preimage)
}

// AddRefund adds gas to the refund counter
func (s *MVCCStateDB) AddRefund(gas uint64) {
	s.baseState.AddRefund(gas)
}

// SubRefund removes gas from the refund counter
func (s *MVCCStateDB) SubRefund(gas uint64) {
	s.baseState.SubRefund(gas)
}

// GetRefund returns the current value of the refund counter
func (s *MVCCStateDB) GetRefund() uint64 {
	return s.baseState.GetRefund()
}

// GetBaseState returns the underlying state.StateDB
func (s *MVCCStateDB) GetBaseState() *state.StateDB {
	return s.baseState
}

// CreateAccount creates a new account
func (s *MVCCStateDB) CreateAccount(addr common.Address) {
	s.trackWrite(addr, "account")
	s.baseState.CreateAccount(addr)
}

// Empty returns whether the account is considered empty
func (s *MVCCStateDB) Empty(addr common.Address) bool {
	return s.baseState.Empty(addr)
}

// Exist reports whether the given account exists in state.
// Notably this should also return true for suicided accounts.
func (s *MVCCStateDB) Exist(addr common.Address) bool {
	return s.baseState.Exist(addr)
}

// ForEachStorage iterates over each storage item and calls the provided callback
func (s *MVCCStateDB) ForEachStorage(addr common.Address, cb func(key, value common.Hash) bool) error {
	return s.baseState.ForEachStorage(addr, cb)
}

// GetCode returns the contract code associated with this object
func (s *MVCCStateDB) GetCode(addr common.Address) []byte {
	s.trackRead(addr, "code")
	hash := s.GetCodeHash(addr)
	if hash == (common.Hash{}) {
		return nil
	}
	return s.baseState.GetCode(addr)
}

// GetCodeSize returns the size of the contract code
func (s *MVCCStateDB) GetCodeSize(addr common.Address) int {
	return len(s.GetCode(addr))
}

// GetCodeHash returns the code hash
func (s *MVCCStateDB) GetCodeHash(addr common.Address) common.Hash {
	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "codehash", s.baseState)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
		return common.Hash{}
	}
	return value.(common.Hash)
}

// SetCode sets the contract code
func (s *MVCCStateDB) SetCode(addr common.Address, code []byte) {
	s.trackWrite(addr, "code")
	s.baseState.SetCode(addr, code)
}

// GetCommittedState returns the committed state of the given account
func (s *MVCCStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	return s.baseState.GetCommittedState(addr, hash)
}

// HasSuicided returns whether the given account has been suicided
func (s *MVCCStateDB) HasSuicided(addr common.Address) bool {
	return s.GetSuicided(addr)
}

// Suicide marks the given account as suicided
func (s *MVCCStateDB) Suicide(addr common.Address) bool {
	s.trackWrite(addr, "suicided")
	s.mvccManager.WriteAddressState(s.txIndex, addr, "suicided", true)
	return true
}

// PrepareAccessList handles the preparatory steps for executing a state transition with
// regards to both EIP-2929 and EIP-2930
func (s *MVCCStateDB) PrepareAccessList(sender common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	s.baseState.PrepareAccessList(sender, dest, precompiles, txAccesses)
}

// Snapshot returns an identifier for the current revision of the state
func (s *MVCCStateDB) Snapshot() int {
	return s.baseState.Snapshot()
}

// RevertToSnapshot reverts all state changes made since the given revision
func (s *MVCCStateDB) RevertToSnapshot(id int) {
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
		// 写入失败，中止交易
		db.mvccState.mvccManager.AbortTransaction(db.txIndex)
	}
}

// ReadStorageState 读取存储状态，支持MVCC
func (w *MVCCStateDB) ReadStorageState(addr common.Address, key common.Hash, txIndex int) common.Hash {
	// 从MVCC管理器读取
	value, success := w.mvccManager.ReadStorageState(txIndex, addr, key, w.baseState)
	if !success {
		// 读取失败，返回空值
		return common.Hash{}
	}

	// 检查是否是被中止交易的nil值
	if value == nil {
		log.Debug("读取到nil值，表示依赖于被中止的交易",
			"reader", txIndex,
			"address", addr,
			"key", key)

		// 返回特殊值表示需要等待
		return common.Hash{0xff} // 特殊标记，表示需要等待
	}

	return value.(common.Hash)
}

func (s *MVCCStateDB) GetSuicided(addr common.Address) bool {
	value, success := s.mvccManager.ReadAddressState(s.txIndex, addr, "suicided", s.baseState)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
		return false
	}
	return value.(bool)
}

func (s *MVCCStateDB) SetSuicided(addr common.Address, suicided bool) {
	success := s.mvccManager.WriteAddressState(s.txIndex, addr, "suicided", suicided)
	if !success {
		s.mvccManager.AbortTransaction(s.txIndex)
	}
}

func (s *MVCCStateDB) GetMVCCManager() *MVCCStateManager {
	return s.mvccManager
}
