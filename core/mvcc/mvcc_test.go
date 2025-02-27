package mvcc

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

// TestMVCCBasic 测试基本的MVCC功能
func TestMVCCBasic(t *testing.T) {
	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 创建MVCC管理器
	mvccManager := NewMVCCStateManager(0)

	// 测试地址和键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.BigToHash(big.NewInt(1))

	// 初始化合约存储状态
	initialValue := big.NewInt(100)
	stateDB.SetState(contractAddr, storageKey, common.BigToHash(initialValue))
	stateDB.Commit(false)

	// 测试写入
	tx0Value := common.BigToHash(big.NewInt(1000))
	success := mvccManager.WriteStorageState(0, contractAddr, storageKey, tx0Value)
	if !success {
		t.Fatalf("写入交易0失败")
	}

	// 测试读取
	value1, ok := mvccManager.ReadStorageState(1, contractAddr, storageKey, stateDB)
	if !ok {
		t.Fatalf("读取交易1失败")
	}
	if value1.(common.Hash).Big().Int64() != 1000 {
		t.Fatalf("交易1读取值错误，期望1000，实际%d", value1.(common.Hash).Big().Int64())
	}

	// 测试写入后读取
	tx2Value := common.BigToHash(big.NewInt(1002))
	success = mvccManager.WriteStorageState(2, contractAddr, storageKey, tx2Value)
	if !success {
		t.Fatalf("写入交易2失败")
	}

	value3, ok := mvccManager.ReadStorageState(3, contractAddr, storageKey, stateDB)
	if !ok {
		t.Fatalf("读取交易3失败")
	}
	if value3.(common.Hash).Big().Int64() != 1002 {
		t.Fatalf("交易3读取值错误，期望1002，实际%d", value3.(common.Hash).Big().Int64())
	}

	// 测试提交到状态
	mvccManager.CommitToState(contractAddr, storageKey, stateDB)
	finalState := stateDB.GetState(contractAddr, storageKey)
	if finalState.Big().Int64() != 1002 {
		t.Fatalf("最终状态错误，期望1002，实际%d", finalState.Big().Int64())
	}

	t.Logf("MVCC基本测试通过")
}

// TestMVCCConflict 测试MVCC冲突检测
func TestMVCCConflict(t *testing.T) {
	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 创建MVCC管理器
	mvccManager := NewMVCCStateManager(0)

	// 测试地址和键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.BigToHash(big.NewInt(1))

	// 初始化合约存储状态
	initialValue := big.NewInt(100)
	stateDB.SetState(contractAddr, storageKey, common.BigToHash(initialValue))
	stateDB.Commit(false)

	// 交易5读取初始值
	value5, ok := mvccManager.ReadStorageState(5, contractAddr, storageKey, stateDB)
	if !ok {
		t.Fatalf("读取交易5失败")
	}
	if value5.(common.Hash).Big().Int64() != 100 {
		t.Fatalf("交易5读取值错误，期望100，实际%d", value5.(common.Hash).Big().Int64())
	}

	// 交易2写入新值
	tx2Value := common.BigToHash(big.NewInt(1002))
	success := mvccManager.WriteStorageState(2, contractAddr, storageKey, tx2Value)
	if !success {
		t.Fatalf("写入交易2失败")
	}

	// 交易5应该被中止，因为它读取了旧值
	if !mvccManager.IsTransactionAborted(5) {
		t.Fatalf("交易5应该被中止")
	}

	// 交易7读取交易2的值
	value7, ok := mvccManager.ReadStorageState(7, contractAddr, storageKey, stateDB)
	if !ok {
		t.Fatalf("读取交易7失败")
	}
	if value7.(common.Hash).Big().Int64() != 1002 {
		t.Fatalf("交易7读取值错误，期望1002，实际%d", value7.(common.Hash).Big().Int64())
	}

	// 交易4写入新值
	tx4Value := common.BigToHash(big.NewInt(1004))
	success = mvccManager.WriteStorageState(4, contractAddr, storageKey, tx4Value)
	if !success {
		t.Fatalf("写入交易4失败")
	}

	// 交易7应该被中止，因为它读取了交易2的值，而交易4写入了新值
	if !mvccManager.IsTransactionAborted(7) {
		t.Fatalf("交易7应该被中止")
	}

	t.Logf("MVCC冲突检测测试通过")
}
