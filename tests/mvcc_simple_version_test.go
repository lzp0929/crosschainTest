package tests

import (
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/mvcc"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

// TestMVCCSingleTransaction 测试单笔交易的MVCC操作
func TestMVCCSingleTransaction(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_single_tx_test.log")
	if err != nil {
		t.Fatalf("无法创建日志文件: %v", err)
	}
	defer logFile.Close()

	// 写入日志头
	fmt.Fprintf(logFile, "MVCC单笔交易测试\n")
	fmt.Fprintf(logFile, "==============\n\n")

	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 创建MVCC管理器，只有一笔交易
	txCount := 1
	mvccManager := mvcc.NewMVCCStateManager(txCount)

	// 测试地址和键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.BigToHash(big.NewInt(1))

	// 初始化合约存储状态
	initialValue := big.NewInt(100)
	stateDB.SetState(contractAddr, storageKey, common.BigToHash(initialValue))
	stateDB.Commit(false)

	fmt.Fprintf(logFile, "初始合约状态: 地址=%s, 键=%s, 值=%d\n\n",
		contractAddr.Hex(), storageKey.Hex(), initialValue)

	// 执行交易 (txIndex = 0)
	txIndex := 0
	fmt.Fprintf(logFile, "执行交易 %d\n", txIndex)

	// 1. 读取操作 - 从多版本状态表中读取，如果表为空则从状态快照中读取
	fmt.Fprintf(logFile, "步骤1: 读取操作\n")
	readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
	if !ok {
		t.Fatalf("读取操作失败")
	}

	readHash := readValue.(common.Hash)
	readInt := readHash.Big().Int64()
	fmt.Fprintf(logFile, "  读取值: %d\n", readInt)
	fmt.Fprintf(logFile, "  读取来源: %s\n", "状态快照 (多版本状态表为空)")

	// 2. 写入操作 - 将新值写入多版本状态表
	fmt.Fprintf(logFile, "步骤2: 写入操作\n")
	newValue := big.NewInt(200)
	newValueHash := common.BigToHash(newValue)
	success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)
	if !success {
		t.Fatalf("写入操作失败")
	}
	fmt.Fprintf(logFile, "  写入值: %d\n", newValue)

	// 3. 获取并显示多版本状态表内容
	fmt.Fprintf(logFile, "\n多版本状态表内容:\n")
	versions := mvccManager.GetVersions(contractAddr, storageKey)
	if versions == nil || len(versions) == 0 {
		fmt.Fprintf(logFile, "  多版本状态表为空\n")
	} else {
		fmt.Fprintf(logFile, "  版本数量: %d\n", len(versions))
		for i, version := range versions {
			valueHash := version.Value.(common.Hash)
			fmt.Fprintf(logFile, "  版本 %d:\n", i)
			fmt.Fprintf(logFile, "    交易序号: %d\n", version.TxIndex)
			fmt.Fprintf(logFile, "    值: %d\n", valueHash.Big().Int64())
			fmt.Fprintf(logFile, "    是否中止: %v\n", version.IsAborted)
		}
	}

	// 4. 再次读取，这次应该从多版本状态表中读取
	fmt.Fprintf(logFile, "\n步骤4: 再次读取操作\n")
	readValue2, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
	if !ok {
		t.Fatalf("第二次读取操作失败")
	}

	readHash2 := readValue2.(common.Hash)
	readInt2 := readHash2.Big().Int64()
	fmt.Fprintf(logFile, "  读取值: %d\n", readInt2)
	fmt.Fprintf(logFile, "  读取来源: %s\n", "多版本状态表")

	fmt.Fprintf(logFile, "\n测试完成!\n")
	t.Logf("MVCC单笔交易测试完成，详细结果保存在文件: mvcc_single_tx_test.log")
}
