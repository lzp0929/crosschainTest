package mvcc

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

func TestMVCCReadWrite(t *testing.T) {
	// 创建测试状态
	db := rawdb.NewMemoryDatabase()
	baseState, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 设置测试账户初始状态
	testAddr := common.HexToAddress("0x1234")
	baseState.SetBalance(testAddr, big.NewInt(1000))

	// 创建MVCC管理器
	manager := NewMVCCStateManager(3) // 三个交易

	// 测试写入和读取
	// tx0写入余额
	manager.WriteAddressState(0, testAddr, "balance", big.NewInt(2000))

	// tx1读取余额(应该看到tx0的写入)
	value, success := manager.ReadAddressState(1, testAddr, "balance", baseState)
	if !success {
		t.Fatalf("读取应该成功")
	}
	balance := value.(*big.Int)
	if balance.Cmp(big.NewInt(2000)) != 0 {
		t.Fatalf("预期余额2000，实际得到%v", balance)
	}

	// tx2写入余额
	manager.WriteAddressState(2, testAddr, "balance", big.NewInt(3000))

	// tx1再次读取(不应受tx2影响)
	value, success = manager.ReadAddressState(1, testAddr, "balance", baseState)
	balance = value.(*big.Int)
	if balance.Cmp(big.NewInt(2000)) != 0 {
		t.Fatalf("tx1预期读取余额2000，实际得到%v", balance)
	}
}

func TestConflictDetection(t *testing.T) {
	// 创建测试状态
	db := rawdb.NewMemoryDatabase()
	baseState, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 创建MVCC管理器
	manager := NewMVCCStateManager(3)

	// 设置测试账户
	testAddr := common.HexToAddress("0x1234")

	// 模拟冲突场景: tx1读取，tx0写入，应该导致tx1中止
	// tx1先读取余额(从baseState读取)
	_, _ = manager.ReadAddressState(1, testAddr, "balance", baseState)

	// tx0写入余额
	manager.WriteAddressState(0, testAddr, "balance", big.NewInt(500))

	// 检测冲突
	noConflict := manager.DetectAndHandleConflicts(0)
	if noConflict {
		t.Fatalf("应该检测到冲突但未检测到")
	}

	// 检查tx1是否被中止
	aborted := manager.GetAbortedTransactions()
	if len(aborted) != 1 || aborted[0] != 1 {
		t.Fatalf("tx1应该被中止")
	}
}
