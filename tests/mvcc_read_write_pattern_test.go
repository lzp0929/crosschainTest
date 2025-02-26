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

// TestMVCCReadWritePattern tests 4 threads concurrently executing 4 transactions in a read-write pattern
// Transaction 1: Write, Transaction 2: Read, Transaction 3: Write, Transaction 4: Read
func TestMVCCReadWritePattern(t *testing.T) {
	// Create log file
	logFile, err := os.Create("mvcc_read_write_pattern_test.log")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()

	// Write log header
	fmt.Fprintf(logFile, "MVCC Read-Write Pattern Test\n")
	fmt.Fprintf(logFile, "==========================\n\n")

	// Create memory database and state DB for testing
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// Create MVCC manager for 4 transactions
	txCount := 4
	mvccManager := mvcc.NewMVCCStateManager(txCount)

	// Test address and key
	contractAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	storageKey := common.BigToHash(big.NewInt(1))

	// Initialize contract storage state
	initialValue := big.NewInt(100)
	stateDB.SetState(contractAddr, storageKey, common.BigToHash(initialValue))
	stateDB.Commit(false)

	fmt.Fprintf(logFile, "Initial contract state: Address=%s, Key=%s, Value=%d\n\n",
		contractAddr.Hex(), storageKey.Hex(), initialValue)

	// Structure for recording transaction execution results
	type txResult struct {
		txIndex     int
		threadID    int
		txType      string // "read" or "write"
		readValue   int64
		writeValue  int64
		success     bool
		executionMs int64
		startTime   time.Time
		endTime     time.Time
	}

	// Create wait group and results channel
	var wg sync.WaitGroup
	resultsCh := make(chan txResult, txCount)

	// Define transaction types
	txTypes := []string{"write", "read", "write", "read"}

	// Start 4 worker threads, each executing one transaction
	fmt.Fprintf(logFile, "Starting 4 worker threads to execute 4 transactions in a read-write pattern\n\n")

	for i := 0; i < txCount; i++ {
		wg.Add(1)
		go func(txIndex int) {
			defer wg.Done()

			// Record thread ID (goroutine ID is not directly accessible, using txIndex as proxy)
			threadID := txIndex + 1

			// Record start time
			startTime := time.Now()

			// Create result object
			result := txResult{
				txIndex:   txIndex,
				threadID:  threadID,
				txType:    txTypes[txIndex],
				startTime: startTime,
				success:   true,
			}

			// 1. Read operation (all transactions need to read first)
			readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
			if !ok {
				fmt.Fprintf(logFile, "Transaction %d (Thread %d): Read operation failed\n", txIndex, threadID)
				result.success = false
				result.endTime = time.Now()
				result.executionMs = result.endTime.Sub(startTime).Milliseconds()
				resultsCh <- result
				return
			}

			readHash := readValue.(common.Hash)
			result.readValue = readHash.Big().Int64()

			// Simulate some processing time with different delays based on transaction index
			// This creates a more realistic concurrent execution pattern
			time.Sleep(time.Duration(5*(txIndex+1)) * time.Millisecond)

			// 2. Write operation (only for write transactions)
			if result.txType == "write" {
				result.writeValue = int64(1000 + txIndex) // Each write transaction writes a different value
				newValueHash := common.BigToHash(big.NewInt(result.writeValue))
				success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)
				if !success {
					fmt.Fprintf(logFile, "Transaction %d (Thread %d): Write operation failed\n", txIndex, threadID)
					result.success = false
				}
			}

			// Record end time and execution duration
			result.endTime = time.Now()
			result.executionMs = result.endTime.Sub(startTime).Milliseconds()

			// Send result
			resultsCh <- result
		}(i)
	}

	// Wait for all worker threads to complete
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results
	results := make([]txResult, 0, txCount)
	for result := range resultsCh {
		results = append(results, result)
	}

	// Sort results by transaction index
	sortedResults := make([]txResult, txCount)
	for _, result := range results {
		sortedResults[result.txIndex] = result
	}

	// Output transaction execution results
	fmt.Fprintf(logFile, "Transaction Execution Results:\n")
	for i, result := range sortedResults {
		fmt.Fprintf(logFile, "Transaction %d (Thread %d):\n", i, result.threadID)
		fmt.Fprintf(logFile, "  Type: %s\n", result.txType)
		fmt.Fprintf(logFile, "  Start Time: %s\n", result.startTime.Format("15:04:05.000000"))
		fmt.Fprintf(logFile, "  End Time: %s\n", result.endTime.Format("15:04:05.000000"))
		fmt.Fprintf(logFile, "  Read Value: %d\n", result.readValue)
		if result.txType == "write" {
			fmt.Fprintf(logFile, "  Write Value: %d\n", result.writeValue)
		}
		fmt.Fprintf(logFile, "  Execution Success: %v\n", result.success)
		fmt.Fprintf(logFile, "  Execution Time: %d ms\n", result.executionMs)
	}

	// Get and display multi-version state table contents
	fmt.Fprintf(logFile, "\nMulti-Version State Table Contents:\n")
	versions := mvccManager.GetVersions(contractAddr, storageKey)
	if versions == nil || len(versions) == 0 {
		fmt.Fprintf(logFile, "  Multi-version state table is empty\n")
	} else {
		fmt.Fprintf(logFile, "  Number of versions: %d\n", len(versions))
		for i, version := range versions {
			fmt.Fprintf(logFile, "  Version %d:\n", i)
			fmt.Fprintf(logFile, "    Transaction Index: %d\n", version.TxIndex)

			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "    Value: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Fprintf(logFile, "    Value: nil (aborted)\n")
			}

			fmt.Fprintf(logFile, "    Is Aborted: %v\n", version.IsAborted)

			// Display reader information
			if version.Readers != nil && len(version.Readers) > 0 {
				fmt.Fprintf(logFile, "    Transactions that read this version: ")
				for readerTx := range version.Readers {
					fmt.Fprintf(logFile, "%d ", readerTx)
				}
				fmt.Fprintf(logFile, "\n")
			} else {
				fmt.Fprintf(logFile, "    Transactions that read this version: None\n")
			}
		}
	}

	// Get aborted transactions
	abortedTxs := mvccManager.GetAbortedTransactions()
	fmt.Fprintf(logFile, "\nAborted Transactions: %v\n", abortedTxs)

	// Add detailed analysis of multi-version state table
	fmt.Fprintf(logFile, "\nMulti-Version State Table Analysis:\n")
	fmt.Fprintf(logFile, "  Block Number: %d\n", mvccManager.GetBlockNumber())

	// Analyze version history
	if len(versions) > 0 {
		fmt.Fprintf(logFile, "  Version History:\n")
		for i, version := range versions {
			fmt.Fprintf(logFile, "  - Version %d:\n", i)
			fmt.Fprintf(logFile, "    Version Number (Transaction Index): %d\n", version.TxIndex)

			if version.Value != nil {
				valueHash := version.Value.(common.Hash)
				fmt.Fprintf(logFile, "    Value: %d\n", valueHash.Big().Int64())
			} else {
				fmt.Fprintf(logFile, "    Value: nil (aborted)\n")
			}

			fmt.Fprintf(logFile, "    Is Aborted: %v\n", version.IsAborted)

			// Display reader information
			fmt.Fprintf(logFile, "    Transactions that read this version: ")
			if version.Readers != nil && len(version.Readers) > 0 {
				for readerTx := range version.Readers {
					fmt.Fprintf(logFile, "%d ", readerTx)
				}
			} else {
				fmt.Fprintf(logFile, "None")
			}
			fmt.Fprintf(logFile, "\n")
		}
	}

	// Add execution timeline analysis
	fmt.Fprintf(logFile, "\nExecution Timeline Analysis:\n")
	fmt.Fprintf(logFile, "  Transaction execution order based on start times:\n")

	// Create a copy of results for timeline sorting
	timelineResults := make([]txResult, len(sortedResults))
	copy(timelineResults, sortedResults)

	// Sort by start time
	for i := 0; i < len(timelineResults); i++ {
		for j := i + 1; j < len(timelineResults); j++ {
			if timelineResults[i].startTime.After(timelineResults[j].startTime) {
				timelineResults[i], timelineResults[j] = timelineResults[j], timelineResults[i]
			}
		}
	}

	for i, result := range timelineResults {
		fmt.Fprintf(logFile, "  %d. Transaction %d (Thread %d, %s) - Started at: %s, Ended at: %s\n",
			i+1, result.txIndex, result.threadID, result.txType,
			result.startTime.Format("15:04:05.000000"),
			result.endTime.Format("15:04:05.000000"))
	}

	fmt.Fprintf(logFile, "\nTest Completed!\n")
	t.Logf("MVCC read-write pattern test completed, detailed results saved in file: mvcc_read_write_pattern_test.log")
}
