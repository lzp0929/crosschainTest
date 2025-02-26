package tests

import (
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/mvcc"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

// TestMVCCConcurrentVersions 测试4个线程并发执行4笔写操作交易
func TestMVCCConcurrentVersions(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_concurrent_versions_test.log")
	if err != nil {
		t.Fatalf("无法创建日志文件: %v", err)
	}
	defer logFile.Close()

	// 写入日志头
	fmt.Fprintf(logFile, "MVCC并发交易版本测试\n")
	fmt.Fprintf(logFile, "===================\n\n")

	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 创建MVCC管理器，4笔交易
	txCount := 4
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

	// 用于记录交易执行结果的结构
	type txResult struct {
		txIndex     int
		readValue   int64
		writeValue  int64
		success     bool
		executionMs int64
	}

	// 创建等待组和结果通道
	var wg sync.WaitGroup
	resultsCh := make(chan txResult, txCount)

	// 启动4个工作线程，每个线程执行一笔交易
	fmt.Fprintf(logFile, "启动4个工作线程执行4笔交易\n\n")

	for i := 0; i < txCount; i++ {
		wg.Add(1)
		go func(txIndex int) {
			defer wg.Done()

			// 记录开始时间
			startTime := time.Now()

			// 创建结果对象
			result := txResult{
				txIndex:    txIndex,
				writeValue: int64(1000 + txIndex), // 每个交易写入不同的值
				success:    true,
			}

			// 1. 读取操作
			readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
			if !ok {
				fmt.Fprintf(logFile, "交易 %d: 读取操作失败\n", txIndex)
				result.success = false
				resultsCh <- result
				return
			}

			readHash := readValue.(common.Hash)
			result.readValue = readHash.Big().Int64()

			// 模拟一些处理时间，使交易有不同的执行顺序
			time.Sleep(time.Duration(10*(txIndex+1)) * time.Millisecond)

			// 2. 写入操作
			newValueHash := common.BigToHash(big.NewInt(result.writeValue))
			success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)
			if !success {
				fmt.Fprintf(logFile, "交易 %d: 写入操作失败\n", txIndex)
				result.success = false
			}

			// 记录执行时间
			result.executionMs = time.Since(startTime).Milliseconds()

			// 发送结果
			resultsCh <- result
		}(i)
	}

	// 等待所有工作线程完成
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// 收集结果
	results := make([]txResult, 0, txCount)
	for result := range resultsCh {
		results = append(results, result)
	}

	// 按交易索引排序结果
	sortedResults := make([]txResult, txCount)
	for _, result := range results {
		sortedResults[result.txIndex] = result
	}

	// 输出交易执行结果
	fmt.Fprintf(logFile, "交易执行结果:\n")
	for i, result := range sortedResults {
		fmt.Fprintf(logFile, "交易 %d:\n", i)
		fmt.Fprintf(logFile, "  读取值: %d\n", result.readValue)
		fmt.Fprintf(logFile, "  写入值: %d\n", result.writeValue)
		fmt.Fprintf(logFile, "  执行成功: %v\n", result.success)
		fmt.Fprintf(logFile, "  执行时间: %d ms\n", result.executionMs)
	}

	// 获取并显示多版本状态表内容
	fmt.Fprintf(logFile, "\n多版本状态表内容:\n")
	versions := mvccManager.GetVersions(contractAddr, storageKey)
	if versions == nil || len(versions) == 0 {
		fmt.Fprintf(logFile, "  多版本状态表为空\n")
	} else {
		fmt.Fprintf(logFile, "  版本数量: %d\n", len(versions))
		for i, version := range versions {
			fmt.Fprintf(logFile, "  版本 %d:\n", i)
			fmt.Fprintf(logFile, "    交易序号: %d\n", version.TxIndex)

			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "    值: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Fprintf(logFile, "    值: nil (已中止)\n")
			}

			fmt.Fprintf(logFile, "    是否中止: %v\n", version.IsAborted)

			// 显示读者信息
			if version.Readers != nil && len(version.Readers) > 0 {
				fmt.Fprintf(logFile, "    读取过该版本的交易: ")
				for readerTx := range version.Readers {
					fmt.Fprintf(logFile, "%d ", readerTx)
				}
				fmt.Fprintf(logFile, "\n")
			} else {
				fmt.Fprintf(logFile, "    读取过该版本的交易: 无\n")
			}
		}
	}

	// 获取被中止的交易
	abortedTxs := mvccManager.GetAbortedTransactions()
	fmt.Fprintf(logFile, "\n被中止的交易: %v\n", abortedTxs)

	// 添加多版本状态表的详细分析
	fmt.Fprintf(logFile, "\n多版本状态表分析:\n")
	fmt.Fprintf(logFile, "  区块号: %d\n", mvccManager.GetBlockNumber())

	// 分析版本历史
	if len(versions) > 0 {
		fmt.Fprintf(logFile, "  版本历史:\n")
		for i, version := range versions {
			fmt.Fprintf(logFile, "  - 版本 %d:\n", i)
			fmt.Fprintf(logFile, "    版本号(交易序号): %d\n", version.TxIndex)

			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "    数值: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Fprintf(logFile, "    数值: nil (已中止)\n")
			}

			// 显示读者信息
			fmt.Fprintf(logFile, "    读取过该版本的交易: ")
			if version.Readers != nil && len(version.Readers) > 0 {
				for readerTx := range version.Readers {
					fmt.Fprintf(logFile, "%d ", readerTx)
				}
			} else {
				fmt.Fprintf(logFile, "无")
			}
			fmt.Fprintf(logFile, "\n")
		}
	}

	fmt.Fprintf(logFile, "\n测试完成!\n")
	t.Logf("MVCC并发交易版本测试完成，详细结果保存在文件: mvcc_concurrent_versions_test.log")
}
