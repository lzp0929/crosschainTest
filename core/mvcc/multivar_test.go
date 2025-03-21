package mvcc

import (
	"fmt"
	"math/big"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

func TestMultiVarTransactions(t *testing.T) {
	// 创建日志文件
	logFile, err := os.Create("mvcc_multiple_vars_test.log")
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
		t.Log(logMsg)
	}

	writeLog("============= 开始多变量并发交易测试 =============")

	// 初始化状态数据库
	db := state.NewDatabase(rawdb.NewMemoryDatabase())
	baseState, _ := state.New(common.Hash{}, db, nil)

	// 初始化MVCC管理器
	mvccManager := NewMVCCStateManager(0)
	mvccManager.debug = true // 启用调试模式
	writeLog("MVCC管理器初始化完成")

	// 合约地址
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// 三个变量的键
	keyA := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001") // 变量a
	keyB := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002") // 变量b
	keyC := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003") // 变量c

	// 创建15个交易
	numTxs := 15
	var wg sync.WaitGroup

	// 记录开始时间
	startTime := time.Now()
	writeLog("测试开始时间: %v", startTime.Format("15:04:05.000"))

	// 创建4个工作线程
	numWorkers := 4

	// 用于记录每个交易的执行时间
	txStartTimes := make(map[int]time.Time)
	var txTimesMutex sync.Mutex

	// 记录每个变量的最终值
	finalValues := make(map[common.Hash]*big.Int)

	// 记录交易执行结果
	type TxResult struct {
		TxIndex      int
		WorkerID     int
		Type         string
		StartTime    time.Time
		EndTime      time.Time
		Variable     string
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

			for txIndex := workerID; txIndex < numTxs; txIndex += numWorkers {
				// 初始化交易执行记录
				txResultsMutex.Lock()
				txResults[txIndex] = &TxResult{
					TxIndex:      txIndex,
					WorkerID:     workerID,
					Type:         "unknown",
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

				// 根据交易ID确定操作类型和目标变量
				var targetKey common.Hash
				var operation string
				var variableName string

				switch txIndex % 15 {
				case 0: // 交易0写入a
					targetKey = keyA
					operation = "write"
					variableName = "a"
				case 1: // 交易1写入b
					targetKey = keyB
					operation = "write"
					variableName = "b"
				case 2: // 交易2写入c
					targetKey = keyC
					operation = "write"
					variableName = "c"
				case 3: // 交易3读取a
					targetKey = keyA
					operation = "read"
					variableName = "a"
				case 4: // 交易4读取b
					targetKey = keyB
					operation = "read"
					variableName = "b"
				case 5: // 交易5读取c
					targetKey = keyC
					operation = "read"
					variableName = "c"
				case 6: // 交易6写入a
					targetKey = keyA
					operation = "write"
					variableName = "a"
				case 7: // 交易7写入b
					targetKey = keyB
					operation = "write"
					variableName = "b"
				case 8: // 交易8读取a
					targetKey = keyA
					operation = "read"
					variableName = "a"
				case 9: // 交易9读取b
					targetKey = keyB
					operation = "read"
					variableName = "b"
				case 10: // 交易10写入c
					targetKey = keyC
					operation = "write"
					variableName = "c"
				case 11: // 交易11读取c
					targetKey = keyC
					operation = "read"
					variableName = "c"
				case 12: // 交易12写入a
					targetKey = keyA
					operation = "write"
					variableName = "a"
				case 13: // 交易13写入c
					targetKey = keyC
					operation = "write"
					variableName = "c"
				case 14: // 交易14读取b
					targetKey = keyB
					operation = "read"
					variableName = "b"
				}

				// 更新交易记录
				txResultsMutex.Lock()
				txResults[txIndex].Type = operation
				txResults[txIndex].Variable = variableName
				txResultsMutex.Unlock()

				writeLog("【交易%d】【线程%d】开始执行交易 (%s 变量%s)",
					txIndex, workerID, operation, variableName)

				// 执行交易
				var success bool
				var readValue *big.Int
				var writeValue *big.Int

				if operation == "read" {
					// 读操作
					value, success := mvccManager.ReadStorageState(txIndex, contractAddr, targetKey, baseState)
					if success {
						if value != nil {
							readValue = value.(common.Hash).Big()
							writeLog("【交易%d】【线程%d】成功读取变量%s的值: %v",
								txIndex, workerID, variableName, readValue)

							txResultsMutex.Lock()
							txResults[txIndex].ReadValue = readValue
							txResults[txIndex].Success = true
							txResultsMutex.Unlock()
						} else {
							writeLog("【交易%d】【线程%d】读取变量%s的值为nil",
								txIndex, workerID, variableName)

							txResultsMutex.Lock()
							txResults[txIndex].Success = true
							txResultsMutex.Unlock()
						}
					} else {
						writeLog("【交易%d】【线程%d】读取变量%s失败",
							txIndex, workerID, variableName)
					}
				} else {
					// 写操作
					writeValue = big.NewInt(int64(txIndex*100 + 1))
					value := common.BigToHash(writeValue)
					success = mvccManager.WriteStorageState(txIndex, contractAddr, targetKey, value)

					txResultsMutex.Lock()
					txResults[txIndex].WriteValue = writeValue
					txResults[txIndex].Success = success
					txResultsMutex.Unlock()

					if success {
						writeLog("【交易%d】【线程%d】成功写入变量%s的值: %v",
							txIndex, workerID, variableName, writeValue)

						// 更新最终值记录
						finalValues[targetKey] = writeValue
					} else {
						writeLog("【交易%d】【线程%d】写入变量%s的值失败",
							txIndex, workerID, variableName)
					}
				}

				// 记录交易结束时间
				txTimesMutex.Lock()
				txResultsMutex.Lock()
				txResults[txIndex].EndTime = time.Now()
				txResultsMutex.Unlock()
				txTimesMutex.Unlock()

				// 等待交易完成
				mvccManager.waitForPreviousTxsFinalized(txIndex)

				writeLog("【交易%d】【线程%d】交易执行完成，状态: %v",
					txIndex, workerID, map[bool]string{true: "成功", false: "失败"}[success])
			}
		}(workerID)
	}

	// 等待所有线程完成
	wg.Wait()

	// 打印执行时间
	totalTime := time.Since(startTime)
	writeLog("\n============= 测试完成 =============")
	writeLog("总执行时间: %v", totalTime)

	// 验证每个变量的最终状态
	writeLog("\n最终变量状态:")

	// 变量a的最终值
	aFinalValue := finalValues[keyA]
	if aFinalValue != nil {
		writeLog("  变量a的最终值: %v", aFinalValue)
	} else {
		writeLog("  变量a的最终值: nil")
	}

	// 变量b的最终值
	bFinalValue := finalValues[keyB]
	if bFinalValue != nil {
		writeLog("  变量b的最终值: %v", bFinalValue)
	} else {
		writeLog("  变量b的最终值: nil")
	}

	// 变量c的最终值
	cFinalValue := finalValues[keyC]
	if cFinalValue != nil {
		writeLog("  变量c的最终值: %v", cFinalValue)
	} else {
		writeLog("  变量c的最终值: nil")
	}

	// 打印交易结果
	writeLog("\n交易执行结果:")
	for txIndex := 0; txIndex < numTxs; txIndex++ {
		result := txResults[txIndex]
		if result != nil {
			writeLog("交易 %d (线程 %d):", txIndex, result.WorkerID)
			writeLog("  类型: %s 变量: %s", result.Type, result.Variable)
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

	// 打印变量a的版本
	aVersions := mvccManager.GetVersions(contractAddr, keyA)
	writeLog("  变量a版本数量: %d", len(aVersions))
	for i, version := range aVersions {
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

	// 打印变量b的版本
	bVersions := mvccManager.GetVersions(contractAddr, keyB)
	writeLog("  变量b版本数量: %d", len(bVersions))
	for i, version := range bVersions {
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

	// 打印变量c的版本
	cVersions := mvccManager.GetVersions(contractAddr, keyC)
	writeLog("  变量c版本数量: %d", len(cVersions))
	for i, version := range cVersions {
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
