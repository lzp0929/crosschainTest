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

// TestMVCCComplexPattern tests 4 threads concurrently executing 8 transactions in a complex pattern
// Thread 1: Tx0, Tx4 (both write)
// Thread 2: Tx1, Tx5 (both read)
// Thread 3: Tx2, Tx6 (both write)
// Thread 4: Tx3, Tx7 (both read)
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
	txCount := 8
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
		"write", // Tx0 - 线程1
		"read",  // Tx1 - 线程2
		"write", // Tx2 - 线程3
		"read",  // Tx3 - 线程4
		"write", // Tx4 - 线程1
		"read",  // Tx5 - 线程2
		"write", // Tx6 - 线程3
		"read",  // Tx7 - 线程4
	}

	// 将线程映射到它们的交易
	threadToTxs := map[int][]int{
		1: {0, 4}, // 线程1 -> Tx0, Tx4
		2: {1, 5}, // 线程2 -> Tx1, Tx5
		3: {2, 6}, // 线程3 -> Tx2, Tx6
		4: {3, 7}, // 线程4 -> Tx3, Tx7
	}

	// 创建等待组和结果通道
	var wg sync.WaitGroup
	resultsCh := make(chan txResult, txCount)

	// 添加完成标志通道，用于防止死锁
	doneCh := make(chan struct{})
	timeoutCh := time.After(20 * time.Second) // 设置20秒超时

	// 启动4个工作线程，每个执行2个交易
	fmt.Fprintf(logFile, "启动4个工作线程执行8个交易的复杂模式\n")
	fmt.Fprintf(logFile, "线程1: Tx0, Tx4 (都是写操作)\n")
	fmt.Fprintf(logFile, "线程2: Tx1, Tx5 (都是读操作)\n")
	fmt.Fprintf(logFile, "线程3: Tx2, Tx6 (都是写操作)\n")
	fmt.Fprintf(logFile, "线程4: Tx3, Tx7 (都是读操作)\n\n")

	// 启动4个工作线程
	for threadID := 1; threadID <= 4; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			// 获取分配给此线程的交易
			threadTxs := threadToTxs[threadID]

			// 在此线程中顺序执行每个交易
			for _, txIndex := range threadTxs {
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

				// 1. 读操作（所有交易都需要先读取）
				var readValue interface{}
				var ok bool

				// 添加超时机制防止死锁
				readDone := make(chan bool, 1)
				go func() {
					readValue, ok = mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
					readDone <- true
				}()

				// 等待读取完成或超时
				select {
				case <-readDone:
					// 读取成功，继续处理
				case <-time.After(2 * time.Second):
					// 读取超时，记录失败并继续
					fmt.Fprintf(logFile, "交易 %d (线程 %d): 读取操作超时\n", txIndex, threadID)
					result.success = false
					result.endTime = time.Now()
					result.executionMs = result.endTime.Sub(startTime).Milliseconds()
					resultsCh <- result
					continue
				}

				if !ok {
					fmt.Fprintf(logFile, "交易 %d (线程 %d): 读取操作失败\n", txIndex, threadID)
					result.success = false
					result.endTime = time.Now()
					result.executionMs = result.endTime.Sub(startTime).Milliseconds()
					resultsCh <- result
					continue
				}

				readHash := readValue.(common.Hash)
				result.readValue = readHash.Big().Int64()

				// 模拟一些处理时间，基于交易索引的不同延迟
				// 这创建了一个更真实的并发执行模式
				time.Sleep(time.Duration(5*(txIndex%4+1)) * time.Millisecond)

				// 2. 写操作（仅对写交易）
				if result.txType == "write" {
					result.writeValue = int64(1000 + txIndex) // 每个写交易写入不同的值
					newValueHash := common.BigToHash(big.NewInt(result.writeValue))

					// 添加超时机制防止死锁
					writeDone := make(chan bool, 1)
					var success bool

					go func() {
						success = mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)
						writeDone <- true
					}()

					// 等待写入完成或超时
					select {
					case <-writeDone:
						// 写入完成，检查结果
						if !success {
							fmt.Fprintf(logFile, "交易 %d (线程 %d): 写入操作失败\n", txIndex, threadID)
							result.success = false
						}
					case <-time.After(2 * time.Second):
						// 写入超时
						fmt.Fprintf(logFile, "交易 %d (线程 %d): 写入操作超时\n", txIndex, threadID)
						result.success = false
					}
				}

				// 记录结束时间和执行持续时间
				result.endTime = time.Now()
				result.executionMs = result.endTime.Sub(startTime).Milliseconds()

				// 发送结果
				resultsCh <- result

				// 在同一线程中的交易之间添加小延迟
				// 这使执行模式更有趣
				if len(threadTxs) > 1 && txIndex == threadTxs[0] {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(threadID)
	}

	// 等待所有工作线程完成或超时
	go func() {
		wg.Wait()
		close(resultsCh)
		close(doneCh)
	}()

	// 收集结果或处理超时
	results := make([]txResult, 0, txCount)

	collectDone := false
	for !collectDone {
		select {
		case result, ok := <-resultsCh:
			if !ok {
				collectDone = true
				break
			}
			results = append(results, result)
		case <-timeoutCh:
			fmt.Fprintf(logFile, "测试执行超时，强制结束\n")
			t.Log("测试执行超时，强制结束")
			collectDone = true
		case <-doneCh:
			collectDone = true
		}
	}

	// 按交易索引排序结果
	sortedResults := make([]txResult, txCount)
	for i := range sortedResults {
		sortedResults[i].success = false // 默认为失败
	}

	for _, result := range results {
		if result.txIndex >= 0 && result.txIndex < txCount {
			sortedResults[result.txIndex] = result
		}
	}

	// 输出交易执行结果
	fmt.Fprintf(logFile, "交易执行结果:\n")
	for i, result := range sortedResults {
		fmt.Fprintf(logFile, "交易 %d (线程 %d):\n", i, result.threadID)
		if !result.success {
			fmt.Fprintf(logFile, "  状态: 失败或未完成\n")
			continue
		}

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

	// 添加超时机制防止死锁
	versionsDone := make(chan bool, 1)
	var versions []mvcc.VersionedValue

	go func() {
		versions = mvccManager.GetVersions(contractAddr, storageKey)
		versionsDone <- true
	}()

	// 等待获取版本完成或超时
	select {
	case <-versionsDone:
		// 获取版本成功，继续处理
	case <-time.After(2 * time.Second):
		// 获取版本超时
		fmt.Fprintf(logFile, "  获取多版本状态表超时\n")
		versions = nil
	}

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
				fmt.Fprintf(logFile, "    值: nil (已中止)\n")
			}

			fmt.Fprintf(logFile, "    是否已中止: %v\n", version.IsAborted)

			// 显示读取者信息
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
	abortedDone := make(chan bool, 1)
	var abortedTxs []int

	go func() {
		abortedTxs = mvccManager.GetAbortedTransactions()
		abortedDone <- true
	}()

	// 等待获取中止交易完成或超时
	select {
	case <-abortedDone:
		// 获取中止交易成功，继续处理
	case <-time.After(2 * time.Second):
		// 获取中止交易超时
		fmt.Fprintf(logFile, "  获取中止交易超时\n")
		abortedTxs = nil
	}

	fmt.Fprintf(logFile, "\n中止的交易: %v\n", abortedTxs)

	// 添加多版本状态表的详细分析
	fmt.Fprintf(logFile, "\n多版本状态表分析:\n")
	fmt.Fprintf(logFile, "  区块号: %d\n", mvccManager.GetBlockNumber())

	// 分析版本历史
	if len(versions) > 0 {
		fmt.Fprintf(logFile, "  版本历史:\n")
		for i, version := range versions {
			fmt.Fprintf(logFile, "  - 版本 %d:\n", i)
			fmt.Fprintf(logFile, "    版本号 (交易索引): %d\n", version.TxIndex)

			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "    值: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Fprintf(logFile, "    值: nil (已中止)\n")
			}

			fmt.Fprintf(logFile, "    是否已中止: %v\n", version.IsAborted)

			// 显示读取者信息
			fmt.Fprintf(logFile, "    读取此版本的交易: ")
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

	// 添加执行时间线分析
	fmt.Fprintf(logFile, "\n执行时间线分析:\n")
	fmt.Fprintf(logFile, "  基于开始时间的交易执行顺序:\n")

	// 创建结果副本用于时间线排序
	var timelineResults []txResult
	for _, result := range sortedResults {
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
	for threadID := 1; threadID <= 4; threadID++ {
		threadTxs := threadToTxs[threadID]
		fmt.Fprintf(logFile, "  线程 %d 执行的交易: ", threadID)
		for _, txIndex := range threadTxs {
			fmt.Fprintf(logFile, "%d ", txIndex)
		}
		fmt.Fprintf(logFile, "\n")

		// 计算线程的总执行时间
		var firstStart time.Time
		var lastEnd time.Time
		first := true
		hasCompletedTx := false

		for _, txIndex := range threadTxs {
			result := sortedResults[txIndex]
			if !result.success {
				continue
			}

			hasCompletedTx = true
			if first || result.startTime.Before(firstStart) {
				firstStart = result.startTime
				first = false
			}
			if result.endTime.After(lastEnd) {
				lastEnd = result.endTime
			}
		}

		if hasCompletedTx {
			totalTime := lastEnd.Sub(firstStart).Milliseconds()
			fmt.Fprintf(logFile, "  线程 %d 总执行时间: %d ms\n", threadID, totalTime)
		} else {
			fmt.Fprintf(logFile, "  线程 %d 没有成功完成任何交易\n", threadID)
		}
	}

	fmt.Fprintf(logFile, "\n测试完成!\n")
	t.Logf("MVCC复杂模式测试完成，详细结果保存在文件: mvcc_complex_pattern_test.log")
}
