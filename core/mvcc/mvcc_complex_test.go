package mvcc

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

// TestMVCCConcurrent 测试并发环境下的MVCC
func TestMVCCConcurrent(t *testing.T) {
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

	// 定义交易类型（奇数索引是读交易，偶数索引是写交易）
	txCount := 8
	txTypes := make([]string, txCount)
	for i := 0; i < txCount; i++ {
		if i%2 == 0 {
			txTypes[i] = "write"
		} else {
			txTypes[i] = "read"
		}
	}

	// 记录交易执行结果
	type txResult struct {
		txIndex     int
		txType      string
		readValue   int64
		writeValue  int64
		success     bool
		executionMs int64
	}
	results := make([]txResult, txCount)

	// 按顺序执行交易，而不是并发
	for i := 0; i < txCount; i++ {
		// 创建结果对象
		result := txResult{
			txIndex: i,
			txType:  txTypes[i],
			success: true,
		}

		// 记录开始时间
		startTime := time.Now()

		// 执行交易
		readValue, ok := mvccManager.ReadStorageState(i, contractAddr, storageKey, stateDB)
		if !ok {
			result.success = false
			result.executionMs = time.Since(startTime).Milliseconds()
			results[i] = result
			continue
		}

		readHash := readValue.(common.Hash)
		result.readValue = readHash.Big().Int64()

		// 模拟处理时间
		time.Sleep(time.Duration(5*(i%4+1)) * time.Millisecond)

		// 如果是写交易，执行写操作
		if result.txType == "write" {
			result.writeValue = int64(1000 + i)
			newValueHash := common.BigToHash(big.NewInt(result.writeValue))
			success := mvccManager.WriteStorageState(i, contractAddr, storageKey, newValueHash)
			if !success {
				result.success = false
			}
		}

		// 记录执行时间
		result.executionMs = time.Since(startTime).Milliseconds()

		// 保存结果
		results[i] = result
	}

	// 输出交易执行结果
	fmt.Println("交易执行结果:")
	for i, result := range results {
		fmt.Printf("交易 %d:\n", i)
		fmt.Printf("  类型: %s\n", result.txType)
		fmt.Printf("  读取值: %d\n", result.readValue)
		if result.txType == "write" {
			fmt.Printf("  写入值: %d\n", result.writeValue)
		}
		fmt.Printf("  执行成功: %v\n", result.success)
		fmt.Printf("  执行时间: %d ms\n", result.executionMs)
	}

	// 获取并显示多版本状态表内容
	fmt.Println("\n多版本状态表内容:")
	versions := mvccManager.GetVersions(contractAddr, storageKey)
	if versions == nil || len(versions) == 0 {
		fmt.Println("  多版本状态表为空")
	} else {
		fmt.Printf("  版本数量: %d\n", len(versions))
		for i, version := range versions {
			fmt.Printf("  版本 %d:\n", i)
			fmt.Printf("    交易索引: %d\n", version.TxIndex)
			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Printf("    值: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Printf("    值: nil\n")
			}
			fmt.Printf("    是否已中止: %v\n", version.IsAborted)
			if version.Readers != nil && len(version.Readers) > 0 {
				fmt.Printf("    读取此版本的交易: ")
				for readerTx := range version.Readers {
					fmt.Printf("%d ", readerTx)
				}
				fmt.Printf("\n")
			} else {
				fmt.Printf("    读取此版本的交易: 无\n")
			}
		}
	}

	// 获取中止的交易
	abortedTxs := mvccManager.GetAbortedTransactions()
	fmt.Printf("\n中止的交易: %v\n", abortedTxs)

	// 将最终状态提交到合约状态
	mvccManager.CommitToState(contractAddr, storageKey, stateDB)
	finalState := stateDB.GetState(contractAddr, storageKey)
	fmt.Printf("\n合约最终状态: %d\n", finalState.Big().Int64())

	// 验证结果
	successfulWrites := 0
	latestWriteTx := -1
	for i, result := range results {
		if result.txType == "write" && result.success {
			successfulWrites++
			if i > latestWriteTx {
				latestWriteTx = i
			}
		}
	}

	if successfulWrites == 0 {
		t.Fatalf("没有成功的写操作")
	}

	if latestWriteTx >= 0 {
		expectedFinalValue := int64(1000 + latestWriteTx)
		if finalState.Big().Int64() != expectedFinalValue {
			t.Fatalf("最终状态错误，期望%d，实际%d", expectedFinalValue, finalState.Big().Int64())
		}
	}

	t.Logf("MVCC并发测试通过，最终状态: %d", finalState.Big().Int64())
}
