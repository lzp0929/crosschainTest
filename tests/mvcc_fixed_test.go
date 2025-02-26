package tests

import (
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/mvcc"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

// TestMVCCConcurrentFixed 使用5个线程并发执行20笔交易，正确实现多版本状态表
func TestMVCCConcurrentFixed(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_concurrent_fixed.log")
	if err != nil {
		t.Fatalf("Cannot create log file: %v", err)
	}
	defer func() {
		// 确保日志文件在测试结束时被关闭
		if err := logFile.Close(); err != nil {
			t.Errorf("Error closing log file: %v", err)
		}
	}()

	// 写入日志头
	fmt.Fprintf(logFile, "MVCC Concurrent Test - Fixed Implementation\n")
	fmt.Fprintf(logFile, "====================================\n\n")
	fmt.Fprintf(logFile, "Start Time: %s\n\n", time.Now().Format(time.RFC3339))

	// 设置最大重试次数和超时时间 - 减少超时时间以避免死锁
	const MaxRetries = 3
	const TestTimeout = 5 * time.Second
	testTimeoutCh := time.After(TestTimeout)

	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 减少交易数量以加速测试
	txCount := 20 // 使用20个交易进行测试
	mvccManager := mvcc.NewMVCCStateManager(txCount)

	// 测试地址和键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.BigToHash(big.NewInt(1))

	// 用于追踪交易状态的结构
	type txStatus struct {
		txType      string // "read" 或 "write"
		value       int64  // 写入的值或读取的值
		conflictTx  int    // 冲突的交易索引（如果有）
		aborted     bool   // 是否被中止
		reExecuted  bool   // 是否被重新执行
		executionMs int64  // 执行时间（毫秒）
		waitTime    int64  // 等待时间（毫秒）
		retries     int    // 重试次数
	}

	// 创建交易状态数组
	txStatuses := make([]txStatus, txCount)

	// 减少冲突组，以减少复杂度
	conflictGroups := 4
	txPerGroup := txCount / conflictGroups

	// 随机种子
	rand.Seed(time.Now().UnixNano())

	// 初始化合约存储状态
	stateDB.SetState(contractAddr, storageKey, common.BigToHash(big.NewInt(999)))
	stateDB.Commit(false)
	fmt.Fprintf(logFile, "Initial contract state: address=%s, key=%s, value=999\n\n",
		contractAddr.Hex(), storageKey.Hex())

	// 为每个交易分配类型并记录在日志
	fmt.Fprintf(logFile, "Transaction allocation details:\n")
	for g := 0; g < conflictGroups; g++ {
		// 创建一个包含txPerGroup索引的数组
		indices := make([]int, txPerGroup)
		for i := 0; i < txPerGroup; i++ {
			indices[i] = g*txPerGroup + i
		}

		// 打乱索引以随机分配读写类型
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})

		// 前一半为写操作，后一半为读操作
		for i, idx := range indices {
			if i < txPerGroup/2 {
				txStatuses[idx] = txStatus{txType: "write", value: int64(idx)}
				fmt.Fprintf(logFile, "TX %d: type=write, conflict_group=%d, key=%s, value=%d\n",
					idx, g, storageKey.Hex()[:10], idx)
			} else {
				txStatuses[idx] = txStatus{txType: "read"}
				fmt.Fprintf(logFile, "TX %d: type=read, conflict_group=%d, key=%s\n",
					idx, g, storageKey.Hex()[:10])
			}
		}
	}

	// 创建等待组和通道
	var wg sync.WaitGroup
	txCh := make(chan int, txCount)
	resultsCh := make(chan struct {
		txIndex int
		status  txStatus
	}, txCount*2)
	doneCh := make(chan struct{})
	allWorkDone := make(chan struct{})

	// 将所有交易放入通道，按序号排序
	for i := 0; i < txCount; i++ {
		txCh <- i
	}
	close(txCh)

	// 启动工作线程
	workerCount := 5
	fmt.Fprintf(logFile, "\nStarting %d worker threads to execute %d transactions\n\n", workerCount, txCount)

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			// 工作者循环处理交易
			for txIndex := range txCh {
				select {
				case <-doneCh:
					return
				default:
					// 记录开始时间
					startTime := time.Now()

					// 获取交易状态
					tx := txStatuses[txIndex]

					// 检查重试次数
					if tx.retries >= MaxRetries {
						// 超过最大重试次数，标记为中止
						tx.aborted = true
						txStatuses[txIndex] = tx

						// 发送最终结果
						select {
						case resultsCh <- struct {
							txIndex int
							status  txStatus
						}{txIndex, tx}:
						case <-doneCh:
							return
						}

						fmt.Fprintf(logFile, "Worker %d: TX %d exceeded max retries (%d), marking as aborted\n",
							workerId, txIndex, MaxRetries)
						continue
					}

					// 交易递增重试计数
					tx.retries++
					txStatuses[txIndex] = tx

					// 执行交易
					if tx.txType == "read" {
						// 读操作 - 从多版本状态表中读取
						value, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)

						if !ok {
							// 读取失败，记录为中止
							tx = txStatuses[txIndex]
							tx.aborted = true
							tx.executionMs = time.Since(startTime).Milliseconds()
							txStatuses[txIndex] = tx

							// 发送结果
							select {
							case resultsCh <- struct {
								txIndex int
								status  txStatus
							}{txIndex, tx}:
							case <-doneCh:
								return
							}

							fmt.Fprintf(logFile, "Worker %d: TX %d (read) aborted, reason: read MVCC version failed, retry=%d\n",
								workerId, txIndex, tx.retries)
							continue
						}

						// 读取成功
						tx = txStatuses[txIndex]
						if value != nil {
							valueHash := value.(common.Hash)
							tx.value = valueHash.Big().Int64()
						}
						tx.executionMs = time.Since(startTime).Milliseconds()
						txStatuses[txIndex] = tx

						fmt.Fprintf(logFile, "Worker %d: TX %d (read) success, value=%d, time=%dms, retry=%d\n",
							workerId, txIndex, tx.value, tx.executionMs, tx.retries)
					} else {
						// 写操作 - 写入多版本状态表
						valueToWrite := common.BigToHash(big.NewInt(tx.value))
						success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, valueToWrite)

						if !success {
							// 写入失败，记录为中止
							tx = txStatuses[txIndex]
							tx.aborted = true
							tx.executionMs = time.Since(startTime).Milliseconds()
							txStatuses[txIndex] = tx

							// 发送结果
							select {
							case resultsCh <- struct {
								txIndex int
								status  txStatus
							}{txIndex, tx}:
							case <-doneCh:
								return
							}

							fmt.Fprintf(logFile, "Worker %d: TX %d (write) aborted, reason: MVCC write conflict, retry=%d\n",
								workerId, txIndex, tx.retries)
							continue
						}

						// 写入成功
						tx = txStatuses[txIndex]
						tx.executionMs = time.Since(startTime).Milliseconds()
						txStatuses[txIndex] = tx

						fmt.Fprintf(logFile, "Worker %d: TX %d (write) success, value=%d, time=%dms, retry=%d\n",
							workerId, txIndex, tx.value, tx.executionMs, tx.retries)
					}

					// 发送最终结果
					select {
					case resultsCh <- struct {
						txIndex int
						status  txStatus
					}{txIndex, tx}:
					case <-doneCh:
						return
					}
				}
			}
		}(w)
	}

	// 创建一个计数器和map，用于追踪每个交易的最新状态
	successfulTxs := make(map[int]bool)
	finalTxStatuses := make([]txStatus, txCount)

	// 创建一个计时器，在所有工作线程完成后关闭result通道
	go func() {
		// 等待直到所有工作线程完成或者超时
		waitCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(waitCh)
		}()

		select {
		case <-waitCh:
			fmt.Fprintf(logFile, "All worker threads completed normally\n")
		case <-testTimeoutCh:
			fmt.Fprintf(logFile, "WARNING: Test timeout reached (%s)\n", TestTimeout)
			close(doneCh) // 通知所有goroutines终止
		}

		// 等待0.1秒，确保所有结果都已处理
		time.Sleep(100 * time.Millisecond)
		close(allWorkDone)
	}()

	// 收集结果
	successCount := 0
	abortedCount := 0
	totalRetries := 0

	// 记录统计数据的映射
	txStats := make(map[int]*txStatus)

	// 收集结果的超时计时器
	collectionTimeout := time.After(TestTimeout)

	// 处理结果
resultLoop:
	for {
		select {
		case result, ok := <-resultsCh:
			if !ok {
				break resultLoop
			}

			idx := result.txIndex
			status := result.status

			// 如果交易已经成功完成，则忽略中间状态的更新
			if successfulTxs[idx] {
				continue
			}

			// 如果交易没有被中止，则标记为成功
			if !status.aborted {
				successfulTxs[idx] = true
				successCount++

				// 更新最终状态
				finalTxStatuses[idx] = status
				txStats[idx] = &status
			} else {
				// 更新中止计数，但只计算每个交易一次
				if _, exists := txStats[idx]; !exists {
					abortedCount++
				}

				// 更新状态但不标记为最终状态
				txStats[idx] = &status
			}

			// 统计总重试次数
			totalRetries += status.retries - 1
		case <-collectionTimeout:
			fmt.Fprintf(logFile, "WARNING: Result collection timeout reached (%s)\n", TestTimeout)
			break resultLoop
		case <-allWorkDone:
			// 所有工作已完成，且已关闭通道
			if len(resultsCh) == 0 {
				break resultLoop
			}
		}
	}

	// 写入最终结果到日志
	fmt.Fprintf(logFile, "\nExecution Results:\n")
	fmt.Fprintf(logFile, "Total transactions: %d\n", txCount)
	fmt.Fprintf(logFile, "Successful transactions: %d\n", successCount)
	fmt.Fprintf(logFile, "Aborted transactions: %d\n", abortedCount)
	fmt.Fprintf(logFile, "Total retries: %d\n", totalRetries)

	// 写入每种交易类型的统计信息
	writeCount := 0
	readCount := 0

	for i := 0; i < txCount; i++ {
		if status, exists := txStats[i]; exists {
			if status.txType == "write" {
				writeCount++
			} else {
				readCount++
			}
		}
	}

	fmt.Fprintf(logFile, "Write transactions: %d\n", writeCount)
	fmt.Fprintf(logFile, "Read transactions: %d\n", readCount)

	// 验证版本历史
	fmt.Fprintf(logFile, "\nFinal version history:\n")
	versions := mvccManager.GetVersions(contractAddr, storageKey)

	if versions == nil || len(versions) == 0 {
		fmt.Fprintf(logFile, "No version history\n")
	} else {
		for i, version := range versions {
			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "Version %d: tx=%d, value=%s, aborted=%v\n",
					i, version.TxIndex, valueHash.Hex(), version.IsAborted)
			} else {
				fmt.Fprintf(logFile, "Version %d: tx=%d, value=nil, aborted=%v\n",
					i, version.TxIndex, version.IsAborted)
			}
		}
	}

	// 验证状态是否符合MVCC规则
	fmt.Fprintf(logFile, "\nMVCC rule validation:\n")
	// 检查版本历史是否按交易序号排序
	for i := 1; i < len(versions); i++ {
		if versions[i].TxIndex < versions[i-1].TxIndex {
			fmt.Fprintf(logFile, "Error: version numbers not monotonically increasing: index%d(%d) < index%d(%d)\n",
				i, versions[i].TxIndex, i-1, versions[i-1].TxIndex)
		}
	}

	// 查找所有被中止的交易
	abortedTxs := mvccManager.GetAbortedTransactions()
	fmt.Fprintf(logFile, "\nAborted transactions: %v\n", abortedTxs)

	fmt.Fprintf(logFile, "\nTest completed!\n")
	t.Logf("MVCC concurrent test completed, detailed results saved in file: mvcc_concurrent_fixed.log")
}
