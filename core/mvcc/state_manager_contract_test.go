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

// TestContractMultiVarAccess 测试多变量访问的并发场景
func TestContractMultiVarAccess(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_contract_test.log")
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

	writeLog("============= 开始多变量合约访问测试 =============")

	// 初始化状态数据库
	db := state.NewDatabase(rawdb.NewMemoryDatabase())
	baseState, _ := state.New(common.Hash{}, db, nil)

	// 初始化MVCC管理器
	mvccManager := NewMVCCStateManager(0)
	mvccManager.debug = true // 启用调试模式
	writeLog("MVCC管理器初始化完成")

	// 合约地址
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// 三个存储键，代表a、b、c三个变量
	storageKeyA := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageKeyB := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
	storageKeyC := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003")

	// 初始化交易计划表
	// 每个交易的操作类型和目标变量
	type TxPlan struct {
		TxID      int
		Operation string      // "read" 或 "write"
		Key       common.Hash // 要操作的变量键
		KeyName   string      // 变量名称，用于日志打印
	}

	txPlans := []TxPlan{
		{0, "write", storageKeyA, "a"},  // 交易0写a
		{1, "write", storageKeyB, "b"},  // 交易1写b
		{2, "write", storageKeyC, "c"},  // 交易2写c
		{3, "read", storageKeyA, "a"},   // 交易3读a
		{4, "read", storageKeyB, "b"},   // 交易4读b
		{5, "read", storageKeyC, "c"},   // 交易5读c
		{6, "write", storageKeyA, "a"},  // 交易6写a
		{7, "write", storageKeyB, "b"},  // 交易7写b
		{8, "read", storageKeyA, "a"},   // 交易8读a
		{9, "read", storageKeyC, "c"},   // 交易9读c
		{10, "write", storageKeyC, "c"}, // 交易10写c
		{11, "read", storageKeyB, "b"},  // 交易11读b
		{12, "write", storageKeyA, "a"}, // 交易12写a
		{13, "write", storageKeyC, "c"}, // 交易13写c
		{14, "read", storageKeyB, "b"},  // 交易14读b
	}

	numTxs := len(txPlans)
	writeLog("计划执行%d笔交易", numTxs)

	// 创建工作线程，比交易数量少，这样可以确保多个交易需要排队
	numWorkers := 4
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
		TxID         int
		WorkerID     int
		Operation    string
		KeyName      string
		StartTime    time.Time
		EndTime      time.Time
		ReadValue    *big.Int
		WriteValue   *big.Int
		Success      bool
		RetryCount   int
		WARConflict  bool
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

				// 获取此交易的操作计划
				plan := txPlans[txIndex]

				// 初始化交易结果记录
				txResultsMutex.Lock()
				txResults[txIndex] = &TxResult{
					TxID:         txIndex,
					WorkerID:     workerID,
					Operation:    plan.Operation,
					KeyName:      plan.KeyName,
					StartTime:    time.Now(),
					Success:      false,
					RetryCount:   0,
					WARConflict:  false,
					WaitedForTxs: []int{},
				}
				txResultsMutex.Unlock()

				// 记录交易开始时间
				txTimesMutex.Lock()
				txStartTimes[txIndex] = time.Now()
				txTimesMutex.Unlock()

				// 获取操作类型和目标键
				isWrite := plan.Operation == "write"
				targetKey := plan.Key
				keyName := plan.KeyName

				writeLog("【交易%d】【线程%d】开始执行交易 (%s %s)",
					txIndex, workerID,
					map[bool]string{true: "写入", false: "读取"}[isWrite],
					keyName)

				recordStatusChange(txIndex, "executing")

				// 创建一个自定义的等待函数，只在检测到WAR冲突时使用
				customWaitForPreviousTxs := func(txIdx int) bool {
					// 只有在检测到冲突时才需要等待
					mvccManager.mu.Lock()
					tx := mvccManager.transactions[txIdx]
					hasConflict := tx != nil && tx.BerunFlag
					var waitForID int
					if hasConflict {
						waitForID, _ = mvccManager.waitForTx[txIdx]
					}
					mvccManager.mu.Unlock()

					if !hasConflict {
						// 没有冲突，不需要等待
						return true
					}

					writeLog("【交易%d】检测到WAR冲突，需要等待交易%d", txIdx, waitForID)
					txResultsMutex.Lock()
					if result := txResults[txIdx]; result != nil {
						result.WARConflict = true
						if !contains(result.WaitedForTxs, waitForID) {
							result.WaitedForTxs = append(result.WaitedForTxs, waitForID)
						}
					}
					txResultsMutex.Unlock()

					startWaitTime := time.Now()
					maxWaitTime := 30 * time.Second

					for {
						if time.Since(startWaitTime) > maxWaitTime {
							writeLog("【交易%d】等待交易%d超时", txIdx, waitForID)
							return false
						}

						// 检查等待的交易是否已完成
						mvccManager.mu.Lock()
						targetTx, exists := mvccManager.transactions[waitForID]
						isFinalized := exists && targetTx.Status == "finalized"
						mvccManager.mu.Unlock()

						if isFinalized {
							writeLog("【交易%d】等待的交易%d已完成", txIdx, waitForID)
							return true
						}

						time.Sleep(10 * time.Millisecond)
					}
				}

				// 创建一个执行函数
				executeFunc := func() error {
					var readValue *big.Int

					if isWrite {
						// 写入操作
						writeValue := big.NewInt(int64(txIndex*100 + 1))
						value := common.BigToHash(writeValue)
						success := mvccManager.WriteStorageState(txIndex, contractAddr, targetKey, value)
						writeLog("【交易%d】【线程%d】写入%s: %v, 结果: %v",
							txIndex, workerID, keyName, writeValue,
							map[bool]string{true: "成功", false: "失败"}[success])

						txResultsMutex.Lock()
						txResults[txIndex].WriteValue = writeValue
						txResultsMutex.Unlock()

						if !success {
							// 如果写入失败，检查是否是WAR冲突
							mvccManager.mu.Lock()
							tx := mvccManager.transactions[txIndex]
							isWARConflict := tx != nil && tx.BerunFlag
							mvccManager.mu.Unlock()

							if isWARConflict {
								// 是WAR冲突，需要等待并重试
								writeLog("【交易%d】【线程%d】写入%s失败，检测到WAR冲突",
									txIndex, workerID, keyName)

								// 等待冲突的前序交易完成
								if !customWaitForPreviousTxs(txIndex) {
									return fmt.Errorf("等待冲突交易超时")
								}

								// 重试写入
								success = mvccManager.WriteStorageState(txIndex, contractAddr, targetKey, value)
								writeLog("【交易%d】【线程%d】冲突解决后重试写入%s: %v, 结果: %v",
									txIndex, workerID, keyName, writeValue,
									map[bool]string{true: "成功", false: "失败"}[success])

								if !success {
									return fmt.Errorf("冲突解决后写入仍失败")
								}
							} else {
								return fmt.Errorf("写入失败，非WAR冲突")
							}
						}
					} else {
						// 读操作
						value, success := mvccManager.ReadStorageState(txIndex, contractAddr, targetKey, baseState)
						if !success {
							// 如果读取失败，检查是否是WAR冲突
							mvccManager.mu.Lock()
							tx := mvccManager.transactions[txIndex]
							isWARConflict := tx != nil && tx.BerunFlag
							mvccManager.mu.Unlock()

							if isWARConflict {
								// 是WAR冲突，需要等待并重试
								writeLog("【交易%d】【线程%d】读取%s失败，检测到WAR冲突",
									txIndex, workerID, keyName)

								// 等待冲突的前序交易完成
								if !customWaitForPreviousTxs(txIndex) {
									return fmt.Errorf("等待冲突交易超时")
								}

								// 重试读取
								value, success = mvccManager.ReadStorageState(txIndex, contractAddr, targetKey, baseState)
								if !success {
									return fmt.Errorf("冲突解决后读取仍失败")
								}
							} else {
								return fmt.Errorf("读取失败，非WAR冲突")
							}
						}

						if value == nil {
							readValue = big.NewInt(0)
							writeLog("【交易%d】【线程%d】读取%s值为nil，使用默认值0",
								txIndex, workerID, keyName)
						} else {
							readValue = value.(common.Hash).Big()
							writeLog("【交易%d】【线程%d】读取%s值: %v",
								txIndex, workerID, keyName, readValue)
						}

						txResultsMutex.Lock()
						txResults[txIndex].ReadValue = readValue
						txResultsMutex.Unlock()
					}
					return nil
				}

				// 执行交易，处理重试和WAR冲突
				err := mvccManager.ExecuteTransaction(txIndex, executeFunc)

				// 记录交易结束时间
				txResultsMutex.Lock()
				result := txResults[txIndex]
				result.EndTime = time.Now()
				txResultsMutex.Unlock()

				if err != nil {
					writeLog("【交易%d】【线程%d】执行出错: %v",
						txIndex, workerID, err)
					recordStatusChange(txIndex, "error: "+err.Error())

					// 尝试重试一次
					if strings.Contains(err.Error(), "需要重试") || strings.Contains(err.Error(), "冲突") {
						writeLog("【交易%d】【线程%d】尝试重新执行", txIndex, workerID)
						retryErr := mvccManager.ExecuteTransaction(txIndex, executeFunc)
						if retryErr == nil {
							writeLog("【交易%d】【线程%d】重试成功", txIndex, workerID)
							err = nil

							txResultsMutex.Lock()
							txResults[txIndex].Success = true
							txResults[txIndex].RetryCount++
							txResultsMutex.Unlock()

							recordStatusChange(txIndex, "retry_success")
						} else {
							writeLog("【交易%d】【线程%d】重试仍失败: %v",
								txIndex, workerID, retryErr)
							recordStatusChange(txIndex, "retry_failed")
						}
					}
				} else {
					writeLog("【交易%d】【线程%d】执行完成", txIndex, workerID)
					recordStatusChange(txIndex, "completed")

					txResultsMutex.Lock()
					txResults[txIndex].Success = true
					txResultsMutex.Unlock()
				}

				// 获取交易状态
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

				writeLog("【交易%d】【线程%d】最终状态: %s, 重试次数: %s, 执行时间: %v",
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
	writeLog("\n最终存储状态:")
	finalValueA := mvccManager.GetLatestValue(contractAddr, storageKeyA)
	if finalValueA != nil {
		writeLog("变量a最终值: %v", finalValueA.(common.Hash).Big())
	} else {
		writeLog("变量a最终值: nil")
	}

	finalValueB := mvccManager.GetLatestValue(contractAddr, storageKeyB)
	if finalValueB != nil {
		writeLog("变量b最终值: %v", finalValueB.(common.Hash).Big())
	} else {
		writeLog("变量b最终值: nil")
	}

	finalValueC := mvccManager.GetLatestValue(contractAddr, storageKeyC)
	if finalValueC != nil {
		writeLog("变量c最终值: %v", finalValueC.(common.Hash).Big())
	} else {
		writeLog("变量c最终值: nil")
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
			writeLog("  操作: %s %s", result.Operation, result.KeyName)
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
			writeLog("  WAR冲突: %v", result.WARConflict)
			writeLog("  等待过的交易: %v", result.WaitedForTxs)
			writeLog("  执行时间: %v", result.EndTime.Sub(result.StartTime))
		}
	}

	// 打印多版本状态表内容
	writeLog("\n多版本状态表内容:")

	printVersions := func(keyName string, key common.Hash) {
		writeLog("\n变量%s的版本:", keyName)
		mvccManager.mu.Lock()
		versions := mvccManager.storageStates[contractAddr][key]
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
	}

	printVersions("a", storageKeyA)
	printVersions("b", storageKeyB)
	printVersions("c", storageKeyC)

	// 打印最终统计信息
	writeLog("\n最终统计信息:")
	successCount := 0
	retryTotal := 0
	warConflictCount := 0

	for txIndex := 0; txIndex < numTxs; txIndex++ {
		result := txResults[txIndex]
		if result != nil {
			if result.Success {
				successCount++
			}
			retryTotal += result.RetryCount
			if result.WARConflict {
				warConflictCount++
			}
		}

		mvccManager.mu.Lock()
		tx := mvccManager.transactions[txIndex]
		mvccManager.mu.Unlock()
		if tx != nil {
			writeLog("  交易%d: 状态=%s, 重试=%d, 总执行时间=%v",
				txIndex, tx.Status, tx.Retries, txResults[txIndex].EndTime.Sub(txResults[txIndex].StartTime))
		}
	}

	writeLog("\n总体统计:")
	writeLog("  成功交易数: %d / %d", successCount, numTxs)
	writeLog("  总重试次数: %d", retryTotal)
	writeLog("  WAR冲突数: %d", warConflictCount)
	writeLog("  总执行时间: %v", totalTime)
}
