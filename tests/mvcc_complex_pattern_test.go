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

// TestMVCCComplexPattern tests concurrent transaction execution with MVCC
func TestMVCCComplexPattern(t *testing.T) {
	// 设置更长的测试超时时间
	if testing.Short() {
		t.Skip("跳过在short模式下运行的复杂测试")
	}

	// 创建日志文件
	logFile, err := os.Create("mvcc_complex_pattern_test.log")
	if err != nil {
		t.Fatalf("创建日志文件失败: %v", err)
	}
	defer logFile.Close()

	// 写入日志头
	fmt.Fprintf(logFile, "MVCC复杂模式测试\n")
	fmt.Fprintf(logFile, "=======================\n\n")

	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 创建MVCC管理器，用于8个交易
	mvccManager := mvcc.NewMVCCStateManager(0)

	// 测试地址和键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.BigToHash(big.NewInt(1))

	// 初始化合约存储状态
	initialValue := big.NewInt(100)
	stateDB.SetState(contractAddr, storageKey, common.BigToHash(initialValue))
	stateDB.Commit(false)

	fmt.Fprintf(logFile, "初始合约状态: 地址=%s, 键=%s, 值=%d\n\n",
		contractAddr.Hex(), storageKey.Hex(), initialValue)

	// 记录交易执行结果的结构
	type txResult struct {
		txIndex     int
		threadID    int
		txType      string // "read" 或 "write"
		readValue   int64
		writeValue  int64
		success     bool
		executionMs int64
		startTime   time.Time
		endTime     time.Time
	}

	// 定义交易类型（奇数索引是读交易，偶数索引是写交易）
	txTypes := []string{
		"write", // Tx0
		"read",  // Tx1
		"write", // Tx2
		"read",  // Tx3
		"write", // Tx4
		"read",  // Tx5
		"write", // Tx6
		"read",  // Tx7
	}

	// 创建优先级队列（使用通道模拟）
	pendingTxs := make([]int, 8)
	for i := 0; i < 8; i++ {
		pendingTxs[i] = i
	}

	// 创建结果通道和结果数组
	resultsCh := make(chan txResult, 8)
	results := make([]txResult, 8)

	// 设置最大并行度
	maxWorkers := 4 // 使用4个工作线程
	var wg sync.WaitGroup

	// 创建互斥锁用于保护pendingTxs
	var txMutex sync.Mutex

	// 启动工作线程
	fmt.Fprintf(logFile, "启动%d个工作线程执行%d个交易\n", maxWorkers, 8)
	fmt.Fprintf(logFile, "交易类型: 偶数索引为写操作，奇数索引为读操作\n\n")

	for threadID := 1; threadID <= maxWorkers; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			for {
				// 获取下一个要执行的交易
				txMutex.Lock()
				if len(pendingTxs) == 0 {
					txMutex.Unlock()
					return
				}

				// 获取最小序号的交易
				minIndex := 0
				for i := 1; i < len(pendingTxs); i++ {
					if pendingTxs[i] < pendingTxs[minIndex] {
						minIndex = i
					}
				}
				txIndex := pendingTxs[minIndex]

				// 从待执行列表中移除该交易
				pendingTxs = append(pendingTxs[:minIndex], pendingTxs[minIndex+1:]...)
				txMutex.Unlock()

				// 记录开始时间
				startTime := time.Now()

				// 创建结果对象
				result := txResult{
					txIndex:   txIndex,
					threadID:  threadID,
					txType:    txTypes[txIndex],
					startTime: startTime,
					success:   true,
				}

				// 执行交易
				readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
				if !ok {
					result.success = false
					result.endTime = time.Now()
					result.executionMs = result.endTime.Sub(startTime).Milliseconds()
					resultsCh <- result
					// 执行完后等待1毫秒
					time.Sleep(1 * time.Millisecond)
					continue
				}

				readHash := readValue.(common.Hash)
				result.readValue = readHash.Big().Int64()

				// 模拟处理时间（根据交易类型调整）
				if result.txType == "write" {
					time.Sleep(10 * time.Millisecond) // 写操作耗时更长
				} else {
					time.Sleep(5 * time.Millisecond) // 读操作耗时较短
				}

				// 如果是写交易，执行写操作
				if result.txType == "write" {
					result.writeValue = int64(1000 + txIndex)
					newValueHash := common.BigToHash(big.NewInt(result.writeValue))
					success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)
					if !success {
						result.success = false
					}
				}

				// 记录结束时间和执行时间
				result.endTime = time.Now()
				result.executionMs = result.endTime.Sub(startTime).Milliseconds()

				// 发送结果
				resultsCh <- result

				// 执行完后等待1毫秒
				time.Sleep(1 * time.Millisecond)
			}
		}(threadID)
	}

	// 等待所有工作线程完成
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// 收集结果
	for result := range resultsCh {
		results[result.txIndex] = result
	}

	// 输出交易执行结果
	fmt.Fprintf(logFile, "交易执行结果:\n")
	for i, result := range results {
		fmt.Fprintf(logFile, "交易 %d (线程 %d):\n", i, result.threadID)
		fmt.Fprintf(logFile, "  类型: %s\n", result.txType)
		fmt.Fprintf(logFile, "  开始时间: %s\n", result.startTime.Format("15:04:05.000000"))
		fmt.Fprintf(logFile, "  结束时间: %s\n", result.endTime.Format("15:04:05.000000"))
		fmt.Fprintf(logFile, "  读取值: %d\n", result.readValue)
		if result.txType == "write" {
			fmt.Fprintf(logFile, "  写入值: %d\n", result.writeValue)
		}
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
			fmt.Fprintf(logFile, "    交易索引: %d\n", version.TxIndex)
			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "    值: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Fprintf(logFile, "    值: nil\n")
			}
			fmt.Fprintf(logFile, "    是否已中止: %v\n", version.IsAborted)
			if version.Readers != nil && len(version.Readers) > 0 {
				fmt.Fprintf(logFile, "    读取此版本的交易: ")
				for readerTx := range version.Readers {
					fmt.Fprintf(logFile, "%d ", readerTx)
				}
				fmt.Fprintf(logFile, "\n")
			} else {
				fmt.Fprintf(logFile, "    读取此版本的交易: 无\n")
			}
		}
	}

	// 获取中止的交易
	abortedTxs := mvccManager.GetAbortedTransactions()
	fmt.Fprintf(logFile, "\n中止的交易分析:\n")
	if len(abortedTxs) == 0 {
		fmt.Fprintf(logFile, "没有交易被中止\n")
	} else {
		fmt.Fprintf(logFile, "中止的交易列表: %v\n", abortedTxs)
		fmt.Fprintf(logFile, "\n中止原因分析:\n")

		// 分析每个被中止的交易
		for _, txIndex := range abortedTxs {
			fmt.Fprintf(logFile, "交易 %d 中止原因分析:\n", txIndex)

			// 获取该交易的执行结果
			tx := results[txIndex]

			// 分析交易类型和读写值
			fmt.Fprintf(logFile, "  - 交易类型: %s\n", tx.txType)
			fmt.Fprintf(logFile, "  - 执行时间: %s\n", tx.startTime.Format("15:04:05.000000"))
			fmt.Fprintf(logFile, "  - 读取的值: %d\n", tx.readValue)
			if tx.txType == "write" {
				fmt.Fprintf(logFile, "  - 尝试写入的值: %d\n", tx.writeValue)
			}

			// 分析依赖关系
			fmt.Fprintf(logFile, "  - 可能的中止原因:\n")

			// 检查是否有更低序号的写入发生在该交易之后
			for _, otherTx := range results {
				if otherTx.txType == "write" && otherTx.txIndex < txIndex &&
					otherTx.startTime.Before(tx.endTime) && otherTx.success {
					fmt.Fprintf(logFile, "    * 交易 %d 在此交易执行期间写入了新值 %d\n",
						otherTx.txIndex, otherTx.writeValue)
				}
			}

			// 检查是否读取了已被覆盖的版本
			versions := mvccManager.GetVersions(contractAddr, storageKey)
			for _, version := range versions {
				if version.TxIndex < txIndex {
					for _, otherTx := range results {
						if otherTx.txType == "write" && otherTx.txIndex < txIndex &&
							otherTx.txIndex > version.TxIndex && otherTx.success {
							fmt.Fprintf(logFile, "    * 读取了交易 %d 的旧版本，但该版本已被交易 %d 覆盖\n",
								version.TxIndex, otherTx.txIndex)
						}
					}
				}
			}

			// 检查是否有读取依赖被中止
			for _, otherTxIndex := range abortedTxs {
				if otherTxIndex < txIndex {
					fmt.Fprintf(logFile, "    * 依赖的交易 %d 被中止\n", otherTxIndex)
				}
			}
		}
	}

	// 将最终状态提交到合约状态
	mvccManager.CommitToState(contractAddr, storageKey, stateDB)
	finalState := stateDB.GetState(contractAddr, storageKey)
	fmt.Fprintf(logFile, "\n合约最终状态: %d\n", finalState.Big().Int64())

	// 添加执行时间线分析
	fmt.Fprintf(logFile, "\n执行时间线分析:\n")
	fmt.Fprintf(logFile, "  基于开始时间的交易执行顺序:\n")
	var timelineResults []txResult
	for _, result := range results {
		if result.success {
			timelineResults = append(timelineResults, result)
		}
	}

	// 按开始时间排序
	for i := 0; i < len(timelineResults); i++ {
		for j := i + 1; j < len(timelineResults); j++ {
			if timelineResults[i].startTime.After(timelineResults[j].startTime) {
				timelineResults[i], timelineResults[j] = timelineResults[j], timelineResults[i]
			}
		}
	}

	for i, result := range timelineResults {
		fmt.Fprintf(logFile, "  %d. 交易 %d (线程 %d, %s) - 开始于: %s, 结束于: %s\n",
			i+1, result.txIndex, result.threadID, result.txType,
			result.startTime.Format("15:04:05.000000"),
			result.endTime.Format("15:04:05.000000"))
	}

	// 添加线程执行分析
	fmt.Fprintf(logFile, "\n线程执行分析:\n")
	threadStats := make(map[int]struct {
		txCount    int
		totalTime  int64
		firstStart time.Time
		lastEnd    time.Time
	})

	for _, result := range results {
		if !result.success {
			continue
		}

		stats := threadStats[result.threadID]
		stats.txCount++
		if stats.txCount == 1 || result.startTime.Before(stats.firstStart) {
			stats.firstStart = result.startTime
		}
		if result.endTime.After(stats.lastEnd) {
			stats.lastEnd = result.endTime
		}
		threadStats[result.threadID] = stats
	}

	for threadID, stats := range threadStats {
		if stats.txCount > 0 {
			totalTime := stats.lastEnd.Sub(stats.firstStart).Milliseconds()
			fmt.Fprintf(logFile, "  线程 %d:\n", threadID)
			fmt.Fprintf(logFile, "    执行交易数: %d\n", stats.txCount)
			fmt.Fprintf(logFile, "    总执行时间: %d ms\n", totalTime)
		}
	}

	fmt.Fprintf(logFile, "\n测试完成!\n")
	t.Logf("MVCC复杂模式测试完成，详细结果保存在文件: mvcc_complex_pattern_test.log")
}
