package tests

import (
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/mvcc"
	"github.com/ethereum/go-ethereum/core/state"
)

// CustomStateDB 是一个包装了StateDB的结构，用于MVCC操作
type CustomStateDB struct {
	StateDB   *state.StateDB
	mvccState *MVCCStateDB
	txIndex   int
}

// MVCCStateDB 管理多版本并发控制的状态
type MVCCStateDB struct {
	mvccManager *mvcc.MVCCStateManager
}

// 创建自定义状态访问器
func createCustomStateDB(statedb *state.StateDB, txMvccStateDB *MVCCStateDB, txIndex int) *CustomStateDB {
	return &CustomStateDB{
		StateDB:   statedb,
		mvccState: txMvccStateDB,
		txIndex:   txIndex,
	}
}

// TestMVCCConcurrent 测试MVCC并发执行
func TestMVCCConcurrent(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_concurrent_test.log")
	if err != nil {
		t.Fatalf("无法创建日志文件: %v", err)
	}
	defer logFile.Close()

	// 写入日志头
	logFile.WriteString("MVCC并发测试\n")
	logFile.WriteString("=============\n\n")
	logFile.WriteString(fmt.Sprintf("开始时间: %s\n\n", time.Now().Format(time.RFC3339)))

	// 创建MVCC状态管理器
	txCount := 10
	mvccManager := mvcc.NewMVCCStateManager(txCount)

	// 测试版本管理
	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	key := common.BigToHash(big.NewInt(1))

	// 写入几个版本
	for i := 0; i < txCount; i++ {
		value := common.BigToHash(big.NewInt(int64(i + 100)))
		mvccManager.WriteStorageState(i, addr, key, value)
		logFile.WriteString(fmt.Sprintf("交易 %d 写入键 1 的值: %s\n", i, value.Hex()))
	}

	// 验证版本历史
	logFile.WriteString("\n验证版本历史:\n")
	versions := mvccManager.GetVersions(addr, key)

	if versions == nil || len(versions) == 0 {
		logFile.WriteString("无版本历史\n")
	} else {
		for i, version := range versions {
			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				logFile.WriteString(fmt.Sprintf("版本 %d: 交易=%d, 值=%s, 中止=%v\n",
					i, version.TxIndex, valueHash.Hex(), version.IsAborted))
			} else {
				logFile.WriteString(fmt.Sprintf("版本 %d: 交易=%d, 值=nil, 中止=%v\n",
					i, version.TxIndex, version.IsAborted))
			}
		}
	}

	logFile.WriteString("\n测试完成!\n")
	t.Logf("MVCC并发测试完成，详细结果保存在文件: mvcc_concurrent_test.log")
}
