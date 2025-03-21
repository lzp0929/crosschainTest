package mvcc

import (
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/log"
)

func init() {
	// 创建日志文件,使用O_TRUNC模式覆盖旧文件
	logFile, err := os.OpenFile("state_manager_test.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(fmt.Sprintf("无法创建日志文件: %v", err))
	}

	// 设置日志格式为简单文本格式
	handler := log.StreamHandler(logFile, log.FormatFunc(func(r *log.Record) []byte {
		timeStr := r.Time.Format("15:04:05.000")
		return []byte(fmt.Sprintf("[%s] %s\n", timeStr, r.Msg))
	}))

	// 同时输出到控制台和文件
	log.Root().SetHandler(log.MultiHandler(
		log.StreamHandler(os.Stdout, log.FormatFunc(func(r *log.Record) []byte {
			timeStr := r.Time.Format("15:04:05.000")
			return []byte(fmt.Sprintf("[%s] %s\n", timeStr, r.Msg))
		})),
		handler,
	))
}

func writeLog(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Info(msg)
}

func TestConcurrentTransactions(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_test.log")
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()

	// 创建一个互斥锁用于保护日志写入
	var logMutex sync.Mutex

	// 写入日志的函数
	writeLog := func(format string, args ...interface{}) {
		logMutex.Lock()
		defer logMutex.Unlock()
		timeStr := time.Now().Format("15:04:05.000")
		logMsg := fmt.Sprintf(format, args...)
		fullMsg := fmt.Sprintf("[%s] %s\n", timeStr, logMsg)
		logFile.WriteString(fullMsg)
		log.Debug(logMsg)
	}

	writeLog("============= 开始并发交易测试 =============")

	// 初始化状态数据库
	db := state.NewDatabase(rawdb.NewMemoryDatabase())
	baseState, _ := state.New(common.Hash{}, db, nil)

	// 初始化MVCC管理器
	mvccManager := NewMVCCStateManager(0)
	mvccManager.debug = true // 启用调试模式
	writeLog("MVCC管理器初始化完成")

	// 合约地址和存储键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")

	// 创建4个工作线程
	numWorkers := 4
	numTxs := 40 // 从20改为8笔交易
	var wg sync.WaitGroup

	// 记录开始时间
	startTime := time.Now()
	writeLog("测试开始时间: %v", startTime.Format("15:04:05.000"))

	// 用于追踪下一个要执行的交易ID
	var nextTxMutex sync.Mutex
	nextTxID := 0

	// 用于记录每个交易的执行时间
	txStartTimes := make(map[int]time.Time)
	var txTimesMutex sync.Mutex

	// 用于记录交易的状态变化
	txStatusChanges := make(map[int][]string)
	var txStatusMutex sync.Mutex

	// 记录交易状态变化的函数
	recordStatusChange := func(txIndex int, status string) {
		txStatusMutex.Lock()
		defer txStatusMutex.Unlock()
		if txStatusChanges[txIndex] == nil {
			txStatusChanges[txIndex] = []string{}
		}
		txStatusChanges[txIndex] = append(txStatusChanges[txIndex], status)
		writeLog("【状态变更】交易%d: %s", txIndex, status)
	}

	// 记录交易执行结果
	type TxResult struct {
		TxIndex      int
		WorkerID     int
		Type         string
		StartTime    time.Time
		EndTime      time.Time
		ReadValue    *big.Int
		WriteValue   *big.Int
		Success      bool
		RetryCount   int
		WaitedForTxs []int
	}
	txResults := make(map[int]*TxResult)
	var txResultsMutex sync.Mutex

	// 启动工作线程
	for workerID := 0; workerID < numWorkers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				// 获取下一个要执行的交易ID
				nextTxMutex.Lock()
				if nextTxID >= numTxs {
					nextTxMutex.Unlock()
					return
				}
				txIndex := nextTxID
				nextTxID++
				nextTxMutex.Unlock()

				// 初始化交易结果记录
				txResultsMutex.Lock()
				txResults[txIndex] = &TxResult{
					TxIndex:      txIndex,
					WorkerID:     workerID,
					Type:         map[bool]string{true: "write", false: "read"}[txIndex%2 == 0],
					StartTime:    time.Now(),
					Success:      false,
					RetryCount:   0,
					WaitedForTxs: []int{},
				}
				txResultsMutex.Unlock()

				// 记录交易开始时间
				txTimesMutex.Lock()
				txStartTimes[txIndex] = time.Now()
				txTimesMutex.Unlock()

				isWrite := txIndex%2 == 0 // 偶数交易写入，奇数交易读取

				writeLog("【交易%d】【线程%d】开始执行交易 (%s)",
					txIndex, workerID,
					map[bool]string{true: "写入", false: "读取"}[isWrite])

				recordStatusChange(txIndex, "executing")

				// 用于记录等待的交易
				waitedForTxs := []int{}

				// 创建一个自定义的等待函数，只在检测到WAR冲突时使用
				customWaitForPreviousTxsFinalized := func(txIdx int) bool {
					if txIdx == 0 {
						// 第一笔交易不需要等待
						log.Debug(fmt.Sprintf("交易%d: 第一笔交易直接标记为finalized", txIdx))
						return true
					}

					writeLog("【交易%d】开始等待前序交易完成", txIdx)
					startWaitTime := time.Now()
					maxWaitTime := 60 * time.Second // 增加到60秒超时

					for {
						if time.Since(startWaitTime) > maxWaitTime {
							writeLog("【交易%d】等待前序交易超时", txIdx)
							return false
						}

						mvccManager.mu.Lock()
						pendingTxs := []int{}
						for i := 0; i < txIdx; i++ {
							tx, exists := mvccManager.transactions[i]
							if !exists {
								// 创建交易
								mvccManager.transactions[i] = &Transaction{
									ID:     i,
									Status: "finalized",
								}
								writeLog("【交易%d】为缺失的前序交易%d创建记录", txIdx, i)
							} else if tx.Status != "finalized" {
								pendingTxs = append(pendingTxs, i)
							}
						}
						mvccManager.mu.Unlock()

						if len(pendingTxs) == 0 {
							writeLog("【交易%d】所有前序交易已完成", txIdx)
							return true
						}

						// 记录等待的交易
						for _, waitTx := range pendingTxs {
							if !contains(waitedForTxs, waitTx) {
								waitedForTxs = append(waitedForTxs, waitTx)
							}
						}

						writeLog("【交易%d】等待前序交易完成: %v", txIdx, pendingTxs)
						time.Sleep(5 * time.Millisecond)
					}
				}

				// 创建一个正常执行函数，不需要等待前序交易
				normalExecute := func() error {
					var readValue *big.Int

					if isWrite {
						// 直接写入，不先读取当前值
						writeValue := big.NewInt(int64(txIndex*100 + 1))
						value := common.BigToHash(writeValue)
						success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, value)
						writeLog("【交易%d】【线程%d】直接写入值: %v, 结果: %v",
							txIndex, workerID, writeValue, map[bool]string{true: "成功", false: "失败"}[success])

						txResultsMutex.Lock()
						txResults[txIndex].WriteValue = writeValue
						txResultsMutex.Unlock()

						if !success {
							return fmt.Errorf("写入失败")
						}
					} else {
						// 读操作
						value, success := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, baseState)
						if !success {
							writeLog("【交易%d】【线程%d】读取失败，需要重试",
								txIndex, workerID)
							return fmt.Errorf("读取失败")
						}
						readValue = value.(common.Hash).Big()
						writeLog("【交易%d】【线程%d】读取到值: %v",
							txIndex, workerID, readValue)

						txResultsMutex.Lock()
						txResults[txIndex].ReadValue = readValue
						txResultsMutex.Unlock()
					}
					return nil
				}

				// 创建一个替代函数，不等待前序交易完成
				var execWithRetry func(int, func() error) error
				execWithRetry = func(txIdx int, executeFn func() error) error {
					// 追踪重试次数以避免死循环
					retryCount := 0
					maxRetries := 10

					mvccManager.mu.Lock()
					if _, exists := mvccManager.transactions[txIdx]; !exists {
						mvccManager.transactions[txIdx] = &Transaction{
							ID:         txIdx,
							Status:     "executing",
							RerunKeys:  make(map[string]struct{}),
							BerunFlag:  false,
							Retries:    0,
							MaxRetries: 10,
						}
						log.Debug(fmt.Sprintf("交易%d: 创建新交易,初始状态为executing", txIdx))
					}

					// 每次执行前都检查是否已被标记为WAR冲突
					tx := mvccManager.transactions[txIdx]
					if tx.BerunFlag {
						// 如果已经被标记为WAR冲突，则需要等待低ID交易完成
						waitForID, exists := mvccManager.waitForTx[txIdx]
						if exists {
							log.Debug(fmt.Sprintf("交易%d: 检测到WAR冲突，需要等待交易%d完成", txIdx, waitForID))

							// 清除BerunFlag标记，准备重试
							tx.BerunFlag = false
							tx.Retries++
							retryCount = tx.Retries
							log.Debug(fmt.Sprintf("交易%d: 重置BerunFlag准备第%d次重试", txIdx, tx.Retries))
							mvccManager.mu.Unlock()

							// 从头开始重试前，先等待一会儿给低ID交易留出执行时间
							// 使用指数退避算法，等待时间随重试次数增加
							waitTime := 20 * time.Millisecond * time.Duration(retryCount+1)
							if waitTime > 200*time.Millisecond {
								waitTime = 200 * time.Millisecond
							}

							writeLog("【交易%d】【线程%d】检测到需要等待依赖交易%d，等待%v后重试",
								txIdx, workerID, waitForID, waitTime)
							recordStatusChange(txIdx, "waiting_for_dependency")

							time.Sleep(waitTime)

							// 如果重试次数过多，放弃此次执行避免死循环
							if retryCount >= maxRetries {
								writeLog("【交易%d】【线程%d】重试次数过多(%d)，放弃执行",
									txIdx, workerID, retryCount)
								return fmt.Errorf("重试次数过多")
							}

							// 递归重试
							return execWithRetry(txIdx, executeFn)
						}
					}
					mvccManager.mu.Unlock()

					// 执行交易逻辑
					err := executeFn()
					if err != nil {
						log.Debug(fmt.Sprintf("交易%d: 交易执行失败, error=%v", txIdx, err))
						return err
					}

					// 在设置状态前立即检查是否被标记为WAR冲突
					mvccManager.mu.Lock()
					tx = mvccManager.transactions[txIdx]

					if tx.BerunFlag {
						retryCount = tx.Retries + 1
						log.Debug(fmt.Sprintf("交易%d: 执行完成后发现BerunFlag=true，需要重新执行", txIdx))
						mvccManager.mu.Unlock()

						// 如果重试次数过多，放弃此次执行避免死循环
						if retryCount >= maxRetries {
							writeLog("【交易%d】【线程%d】执行后检测到冲突，但重试次数过多(%d)，放弃执行",
								txIdx, workerID, retryCount)
							return fmt.Errorf("重试次数过多")
						}

						// 直接递归调用自己进行重试
						writeLog("【交易%d】【线程%d】执行完成后检测到WAR冲突标记，立即重试(第%d次)",
							txIdx, workerID, retryCount)
						recordStatusChange(txIdx, "war_conflict_after_execution")

						// 等待一段时间再重试
						time.Sleep(50 * time.Millisecond)

						return execWithRetry(txIdx, executeFn)
					}

					// 标记交易为completed
					tx.Status = "completed"
					log.Debug(fmt.Sprintf("交易%d: 交易执行完成,状态变更为completed", txIdx))
					mvccManager.mu.Unlock()

					// 确保低ID交易可以完成的特殊处理
					if txIdx > 0 {
						// 检查前序交易状态
						mvccManager.mu.Lock()
						var pendingTxs []int
						for i := 0; i < txIdx; i++ {
							tx, exists := mvccManager.transactions[i]
							if !exists || tx.Status == "executing" {
								pendingTxs = append(pendingTxs, i)
							}
						}
						mvccManager.mu.Unlock()

						// 如果有前序交易未完成，给它们一点时间完成
						if len(pendingTxs) > 0 {
							writeLog("【交易%d】【线程%d】发现前序交易%v未完成，等待100ms",
								txIdx, workerID, pendingTxs)
							time.Sleep(100 * time.Millisecond)
						}
					}

					// 等待所有前序交易完成才能finalize
					writeLog("【交易%d】【线程%d】等待所有前序交易完成后才能finalize", txIdx, workerID)

					// 严格按ID顺序等待所有前序交易
					waitForPrevTxs := func() bool {
						startWait := time.Now()
						maxWaitTime := 5 * time.Second // 减少最大等待时间

						for {
							if time.Since(startWait) > maxWaitTime {
								writeLog("【交易%d】【线程%d】等待前序交易超时", txIdx, workerID)
								return false
							}

							mvccManager.mu.Lock()
							pendingTxs := []int{}
							for i := 0; i < txIdx; i++ {
								tx, exists := mvccManager.transactions[i]
								if !exists {
									// 为了避免死锁，如果交易不存在就创建并标记为已完成
									mvccManager.transactions[i] = &Transaction{
										ID:     i,
										Status: "finalized",
									}
									writeLog("【交易%d】【线程%d】为缺失的前序交易%d创建记录",
										txIdx, workerID, i)
								} else if tx.Status != "finalized" {
									// 必须等待所有前序交易达到finalized状态，不接受completed状态
									pendingTxs = append(pendingTxs, i)
								}
							}
							mvccManager.mu.Unlock()

							if len(pendingTxs) == 0 {
								writeLog("【交易%d】【线程%d】所有前序交易已完成", txIdx, workerID)
								return true
							}

							writeLog("【交易%d】【线程%d】等待前序交易完成: %v", txIdx, workerID, pendingTxs)
							time.Sleep(50 * time.Millisecond)

							// 再次检查自己是否被标记为WAR冲突
							mvccManager.mu.Lock()
							tx := mvccManager.transactions[txIdx]
							if tx != nil && tx.BerunFlag {
								mvccManager.mu.Unlock()
								writeLog("【交易%d】【线程%d】等待前序交易过程中检测到WAR冲突，立即重试",
									txIdx, workerID)
								return false
							}
							mvccManager.mu.Unlock()
						}
					}

					// 等待前序交易，如果过程中检测到WAR冲突则立即重试
					// 如果重试次数过多，则放弃等待直接标记为finalized（避免死锁）
					if retryCount < maxRetries && !waitForPrevTxs() {
						if retryCount+1 >= maxRetries {
							writeLog("【交易%d】【线程%d】重试次数过多，跳过等待，直接标记为finalized",
								txIdx, workerID)
						} else {
							return execWithRetry(txIdx, executeFn)
						}
					}

					// 没有检测到冲突且所有前序交易已完成，标记当前交易为finalized
					mvccManager.mu.Lock()
					// 最后一次检查是否有WAR冲突
					tx = mvccManager.transactions[txIdx]
					if tx != nil && tx.BerunFlag && retryCount < maxRetries {
						mvccManager.mu.Unlock()
						writeLog("【交易%d】【线程%d】最终标记finalized前检测到WAR冲突，立即重试",
							txIdx, workerID)
						recordStatusChange(txIdx, "final_war_check_failed")
						return execWithRetry(txIdx, executeFn)
					}

					// 安全标记为finalized
					if tx != nil {
						tx.Status = "finalized"
						log.Debug(fmt.Sprintf("交易%d: 标记当前交易为finalized", txIdx))
					}
					mvccManager.mu.Unlock()

					return nil
				}

				// 正常执行交易，不需要等待前序交易
				err := execWithRetry(txIndex, normalExecute)

				// 记录交易结束时间
				txResultsMutex.Lock()
				txResults[txIndex].EndTime = time.Now()
				txResults[txIndex].WaitedForTxs = waitedForTxs
				txResultsMutex.Unlock()

				if err != nil {
					// 检查是否是WAR冲突导致的错误
					if strings.Contains(err.Error(), "交易需要重试") {
						writeLog("【交易%d】【线程%d】检测到WAR冲突，准备重试: %v",
							txIndex, workerID, err)

						// 清除交易状态，准备重试
						mvccManager.ClearTransactionState(txIndex)

						// WAR冲突时，使用原始的ExecuteTransaction函数，等待前序交易
						// 重新提交交易，使用原始函数，即等待前序交易
						for retryCount := 0; retryCount < 3; retryCount++ {
							writeLog("【交易%d】【线程%d】第%d次重试开始",
								txIndex, workerID, retryCount+1)

							txResultsMutex.Lock()
							txResults[txIndex].RetryCount = retryCount + 1
							txResultsMutex.Unlock()

							// 在重试时，使用原始执行函数
							retryExecute := func() error {
								// 重试时，使用原始执行函数
								err := normalExecute()
								if err == nil {
									// 确保记录等待状态
									customWaitForPreviousTxsFinalized(txIndex)
								}
								return err
							}

							retryErr := mvccManager.ExecuteTransaction(txIndex, retryExecute)

							if retryErr == nil {
								writeLog("【交易%d】【线程%d】重试成功",
									txIndex, workerID)
								err = nil
								txResultsMutex.Lock()
								txResults[txIndex].Success = true
								txResults[txIndex].EndTime = time.Now()
								txResultsMutex.Unlock()
								break
							} else if strings.Contains(retryErr.Error(), "交易需要重试") {
								writeLog("【交易%d】【线程%d】重试%d失败,仍有冲突: %v",
									txIndex, workerID, retryCount+1, retryErr)
								mvccManager.ClearTransactionState(txIndex)
								// 等待一段时间再重试
								time.Sleep(20 * time.Millisecond * time.Duration(retryCount+1))
							} else {
								writeLog("【交易%d】【线程%d】重试%d失败,放弃: %v",
									txIndex, workerID, retryCount+1, retryErr)
								err = retryErr
								break
							}
						}
					} else {
						writeLog("【交易%d】【线程%d】执行出错: %v",
							txIndex, workerID, err)
					}

					recordStatusChange(txIndex, "error: "+err.Error())
				} else {
					writeLog("【交易%d】【线程%d】执行完成",
						txIndex, workerID)
					recordStatusChange(txIndex, "finalized")

					txResultsMutex.Lock()
					txResults[txIndex].Success = true
					txResultsMutex.Unlock()
				}

				// 打印当前交易的状态
				mvccManager.mu.Lock()
				tx := mvccManager.transactions[txIndex]
				var status, retries string
				if tx != nil {
					status = tx.Status
					retries = fmt.Sprintf("%d", tx.Retries)
				} else {
					status = "unknown"
					retries = "0"
				}
				mvccManager.mu.Unlock()

				writeLog("【交易%d】【线程%d】状态: %s, 重试次数: %s, 执行时间: %v",
					txIndex, workerID, status, retries,
					time.Since(txStartTimes[txIndex]))
			}
		}(workerID)
	}

	// 等待所有线程完成
	wg.Wait()

	// 打印执行时间
	totalTime := time.Since(startTime)
	writeLog("\n============= 测试完成 =============")
	writeLog("总执行时间: %v", totalTime)

	// 验证最终状态
	finalValue := mvccManager.GetLatestValue(contractAddr, storageKey)
	if finalValue != nil {
		writeLog("最终存储值: %v", finalValue.(common.Hash).Big())
	}

	// 打印交易状态变化
	writeLog("\n交易状态变化:")
	var txIndices []int
	for txIndex := range txStatusChanges {
		txIndices = append(txIndices, txIndex)
	}
	sort.Ints(txIndices)
	for _, txIndex := range txIndices {
		writeLog("  交易%d: %s", txIndex, strings.Join(txStatusChanges[txIndex], " -> "))
	}

	// 打印交易结果
	writeLog("\n交易执行结果:")
	for txIndex := 0; txIndex < numTxs; txIndex++ {
		result := txResults[txIndex]
		if result != nil {
			writeLog("交易 %d (线程 %d):", txIndex, result.WorkerID)
			writeLog("  类型: %s", result.Type)
			writeLog("  开始时间: %s", result.StartTime.Format("15:04:05.000000"))
			writeLog("  结束时间: %s", result.EndTime.Format("15:04:05.000000"))

			if result.ReadValue != nil {
				writeLog("  读取值: %v", result.ReadValue)
			}

			if result.WriteValue != nil {
				writeLog("  写入值: %v", result.WriteValue)
			}

			writeLog("  执行成功: %v", result.Success)
			writeLog("  重试次数: %d", result.RetryCount)
			writeLog("  等待过的交易: %v", result.WaitedForTxs)
			writeLog("  执行时间: %v", result.EndTime.Sub(result.StartTime))
		}
	}

	// 打印多版本状态表内容
	writeLog("\n多版本状态表内容:")
	mvccManager.mu.Lock()
	versions := mvccManager.storageStates[contractAddr][storageKey]
	mvccManager.mu.Unlock()

	writeLog("  版本数量: %d", len(versions))
	for i, version := range versions {
		writeLog("  版本 %d:", i)
		writeLog("    交易索引: %d", version.TxIndex)
		if version.Value != nil {
			valueHash := version.Value.(common.Hash)
			writeLog("    值: %v", valueHash.Big())
		} else {
			writeLog("    值: nil")
		}
		writeLog("    是否已中止: %v", version.IsAborted)

		// 打印读者列表
		readers := []int{}
		for reader := range version.Readers {
			readers = append(readers, reader)
		}
		sort.Ints(readers)

		if len(readers) > 0 {
			writeLog("    读取此版本的交易: %v", readers)
		} else {
			writeLog("    读取此版本的交易: 无")
		}
	}

	// 打印最终统计信息
	writeLog("\n最终统计信息:")
	for txIndex := 0; txIndex < numTxs; txIndex++ {
		mvccManager.mu.Lock()
		tx := mvccManager.transactions[txIndex]
		mvccManager.mu.Unlock()
		if tx != nil {
			writeLog("  交易%d: 状态=%s, 重试=%d, 总执行时间=%v",
				txIndex, tx.Status, tx.Retries, time.Since(txStartTimes[txIndex]))
		}
	}
}

// 辅助函数：检查整数切片是否包含特定值
func contains(slice []int, item int) bool {
	for _, value := range slice {
		if value == item {
			return true
		}
	}
	return false
}

func TestSimpleMVCC(t *testing.T) {
	// 初始化状态数据库
	db := state.NewDatabase(rawdb.NewMemoryDatabase())
	baseState, _ := state.New(common.Hash{}, db, nil)

	// 初始化MVCC管理器
	mvccManager := NewMVCCStateManager(0)

	// 合约地址和存储键
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")

	// 执行写操作
	success := mvccManager.WriteStorageState(0, contractAddr, storageKey, common.BigToHash(big.NewInt(42)))
	if !success {
		t.Fatal("写入失败")
	}

	// 执行读操作
	value, success := mvccManager.ReadStorageState(1, contractAddr, storageKey, baseState)
	if !success {
		t.Fatal("读取失败")
	}
	if value.(common.Hash).Big().Int64() != 42 {
		t.Fatalf("读取的值错误，期望42，实际%v", value.(common.Hash).Big().Int64())
	}
}
