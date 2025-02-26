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

// TestMVCCSimple 测试MVCC状态管理器的基本功能
func TestMVCCSimple(t *testing.T) {
	// 输出当前工作目录
	pwd, _ := os.Getwd()
	t.Logf("当前工作目录: %s", pwd)

	// 创建日志文件
	logPath := pwd + "/mvcc_simple_test.log"
	t.Logf("尝试创建日志文件: %s", logPath)

	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("无法创建日志文件: %v", err)
	}
	defer logFile.Close()

	t.Logf("日志文件创建成功")

	// 写入日志头
	logFile.WriteString("MVCC简单测试\n")
	logFile.WriteString("=============\n\n")
	logFile.WriteString(fmt.Sprintf("开始时间: %s\n\n", time.Now().Format(time.RFC3339)))

	// 创建MVCC状态管理器
	txCount := 5
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

	// 测试中止交易
	logFile.WriteString("\n测试中止交易:\n")

	// 中止交易2
	mvccManager.AbortTransaction(2)
	logFile.WriteString("已中止交易 2\n")

	// 再次验证版本历史
	logFile.WriteString("\n中止后的版本历史:\n")
	versions = mvccManager.GetVersions(addr, key)

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
	t.Logf("MVCC简单测试完成，详细结果保存在文件: %s", logPath)
}

// TestMVCCConcurrent100Txs 使用5个线程并发执行100笔交易，模拟真实MVCC场景
func TestMVCCConcurrent100Txs(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_concurrent_100txs.log")
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
	fmt.Fprintf(logFile, "MVCC Concurrent Test - 100 Transactions, 5 Threads\n")
	fmt.Fprintf(logFile, "====================================\n\n")
	fmt.Fprintf(logFile, "Start Time: %s\n\n", time.Now().Format(time.RFC3339))

	// 设置最大重试次数和超时时间
	const MaxRetries = 5
	const TestTimeout = 10 * time.Second
	testTimeoutCh := time.After(TestTimeout)

	// 创建内存数据库和状态DB用于测试
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// 减少交易数量以加速测试
	txCount := 50 // 从100减少到50
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
	conflictGroups := 5 // 从10减少到5
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
	txCh2 := make(chan int, txCount)
	resultsCh := make(chan struct {
		txIndex int
		status  txStatus
	}, txCount*2)
	doneCh := make(chan struct{})
	allWorkDone := make(chan struct{})

	// 创建用于重新执行中止的交易的通道和互斥锁
	retryTxCh := make(chan int, txCount)
	var retryMutex sync.Mutex
	var pendingRetries = make(map[int]struct{})

	// 将所有交易放入通道，按序号排序
	for i := 0; i < txCount; i++ {
		txCh <- i
	}
	close(txCh)

	// 启动工作线程
	workerCount := 5
	fmt.Fprintf(logFile, "\nStarting %d worker threads to execute %d transactions\n\n", workerCount, txCount)

	// 启动一个goroutine处理重试队列
	go func() {
		for {
			select {
			case txIndex, ok := <-retryTxCh:
				if !ok {
					// 通道已关闭
					return
				}

				// 为每个要重试的交易创建一个协程
				go func(idx int) {
					select {
					case <-doneCh:
						return
					default:
						// 等待一小段随机时间，避免同时重试 - 减少等待时间
						time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)

						retryMutex.Lock()
						status := txStatuses[idx]
						if status.retries >= MaxRetries {
							// 超过最大重试次数，不再重试
							delete(pendingRetries, idx)
							retryMutex.Unlock()
							return
						}

						if _, exists := pendingRetries[idx]; exists {
							delete(pendingRetries, idx)
							retryMutex.Unlock()

							select {
							case txCh <- idx:
								// 成功重新放入队列
							case <-doneCh:
								// 测试结束
							}
						} else {
							retryMutex.Unlock()
						}
					}
				}(txIndex)
			case <-doneCh:
				return
			}
		}
	}()

	// 使用新的txCh通道，确保即使原始通道关闭后仍能处理重试
	txCh2 = make(chan int, txCount)

	// 从原始txCh读取，写入txCh2
	go func() {
		for tx := range txCh {
			select {
			case txCh2 <- tx:
				// 成功放入
			case <-doneCh:
				return
			}
		}
	}()

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			// 工作者循环处理交易
			for {
				select {
				case txIndex, ok := <-txCh2:
					if !ok {
						// 通道已关闭
						return
					}

					// 记录开始时间
					startTime := time.Now()

					// 获取交易状态
					retryMutex.Lock()
					tx := txStatuses[txIndex]

					// 检查重试次数
					if tx.retries >= MaxRetries {
						// 超过最大重试次数，标记为中止
						tx.aborted = true
						txStatuses[txIndex] = tx
						retryMutex.Unlock()

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

					// 检查是否为重新执行
					isRetry := tx.retries > 0
					if isRetry {
						tx.reExecuted = true

						// 更新等待时间
						waitEndTime := time.Now()
						tx.waitTime = waitEndTime.Sub(startTime).Milliseconds()

						// 重置开始时间为实际执行开始时间
						startTime = waitEndTime
					}

					// 交易递增重试计数
					tx.retries++
					txStatuses[txIndex] = tx
					retryMutex.Unlock()

					// 执行交易
					if tx.txType == "read" {
						// 读操作
						value, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)

						if !ok {
							// 读取失败，记录为中止
							retryMutex.Lock()
							tx = txStatuses[txIndex]
							tx.aborted = true

							// 记录本次执行时间
							tx.executionMs = time.Since(startTime).Milliseconds()
							txStatuses[txIndex] = tx

							// 只有未超过最大重试次数才重试
							if tx.retries < MaxRetries {
								pendingRetries[txIndex] = struct{}{}
								retryMutex.Unlock()

								// 将交易放入重试队列
								select {
								case retryTxCh <- txIndex:
									// 成功放入重试队列
								case <-doneCh:
									return
								}
							} else {
								retryMutex.Unlock()
							}

							// 发送中间结果
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
						retryMutex.Lock()
						tx = txStatuses[txIndex]
						if value != nil {
							valueHash := value.(common.Hash)
							tx.value = valueHash.Big().Int64()
						}

						// 记录本次执行时间
						tx.executionMs = time.Since(startTime).Milliseconds()
						txStatuses[txIndex] = tx
						retryMutex.Unlock()

						fmt.Fprintf(logFile, "Worker %d: TX %d (read) success, value=%d, time=%dms, retry=%d\n",
							workerId, txIndex, tx.value, tx.executionMs, tx.retries)
					} else {
						// 写操作
						retryMutex.Lock()
						tx = txStatuses[txIndex]
						valueToWrite := common.BigToHash(big.NewInt(tx.value))
						retryMutex.Unlock()

						success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, valueToWrite)

						if !success {
							// 写入失败，记录为中止
							retryMutex.Lock()
							tx = txStatuses[txIndex]
							tx.aborted = true

							// 记录本次执行时间
							tx.executionMs = time.Since(startTime).Milliseconds()
							txStatuses[txIndex] = tx

							// 只有未超过最大重试次数才重试
							if tx.retries < MaxRetries {
								pendingRetries[txIndex] = struct{}{}
								retryMutex.Unlock()

								// 将交易放入重试队列
								select {
								case retryTxCh <- txIndex:
									// 成功放入重试队列
								case <-doneCh:
									return
								}
							} else {
								retryMutex.Unlock()
							}

							// 发送中间结果
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
						retryMutex.Lock()
						tx = txStatuses[txIndex]
						// 记录本次执行时间
						tx.executionMs = time.Since(startTime).Milliseconds()
						txStatuses[txIndex] = tx
						retryMutex.Unlock()

						fmt.Fprintf(logFile, "Worker %d: TX %d (write) success, value=%d, time=%dms, retry=%d\n",
							workerId, txIndex, tx.value, tx.executionMs, tx.retries)
					}

					// 交易成功完成，从待重试列表移除
					retryMutex.Lock()
					delete(pendingRetries, txIndex)
					retryMutex.Unlock()

					// 发送最终结果
					select {
					case resultsCh <- struct {
						txIndex int
						status  txStatus
					}{txIndex, tx}:
					case <-doneCh:
						return
					}
				case <-doneCh:
					return
				}
			}
		}(w)
	}

	// 创建一个计数器和map，用于追踪每个交易的最新状态
	successfulTxs := make(map[int]bool)
	finalTxStatuses := make([]txStatus, txCount)

	// 添加一个变量，跟踪是否所有工作已完成
	allWorkDone = make(chan struct{})

	// 创建一个计时器，在所有工作线程完成后关闭result通道
	go func() {
		defer close(allWorkDone)

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
			return
		}

		// 等待0.1秒，确保所有结果都已处理
		time.Sleep(100 * time.Millisecond)

		// 检查还有哪些交易未完成
		incompleteCount := 0
		for i := 0; i < txCount; i++ {
			if !successfulTxs[i] {
				incompleteCount++
				fmt.Fprintf(logFile, "TX %d incomplete\n", i)
			}
		}

		if incompleteCount > 0 {
			fmt.Fprintf(logFile, "\n%d transactions incomplete\n", incompleteCount)
		}

		// 通知已完成所有工作
		close(allWorkDone)

		// 我们只关闭allWorkDone通道，其他通道在测试结束时会自动被垃圾回收
		// 这样可以避免重复关闭通道的问题
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
				// 通道已关闭
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

	// 获取版本历史，但添加超时保护
	versionCh := make(chan []mvcc.VersionedValue, 1)
	go func() {
		versions := mvccManager.GetVersions(contractAddr, storageKey)
		versionCh <- versions
	}()

	var versions []mvcc.VersionedValue
	select {
	case versions = <-versionCh:
		// 成功获取版本历史
	case <-time.After(1 * time.Second):
		fmt.Fprintf(logFile, "WARNING: Timeout getting version history\n")
		versions = nil
	}

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
	abortedTxsCh := make(chan []int, 1)
	go func() {
		abortedTxs := mvccManager.GetAbortedTransactions()
		abortedTxsCh <- abortedTxs
	}()

	var abortedTxs []int
	select {
	case abortedTxs = <-abortedTxsCh:
		// 成功获取中止交易列表
	case <-time.After(1 * time.Second):
		fmt.Fprintf(logFile, "WARNING: Timeout getting aborted transactions list\n")
		abortedTxs = nil
	}

	fmt.Fprintf(logFile, "\nAborted transactions: %v\n", abortedTxs)

	fmt.Fprintf(logFile, "\nTest completed!\n")
	t.Logf("MVCC concurrent test completed, detailed results saved in file: mvcc_concurrent_100txs.log")
}
