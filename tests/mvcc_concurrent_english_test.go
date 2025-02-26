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

// TestMVCCConcurrentEnglish tests 4 threads concurrently executing 4 write transactions
func TestMVCCConcurrentEnglish(t *testing.T) {
	// Create log file
	logFile, err := os.Create("mvcc_concurrent_english_test.log")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()

	// Write log header
	fmt.Fprintf(logFile, "MVCC Concurrent Transaction Version Test\n")
	fmt.Fprintf(logFile, "======================================\n\n")

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
		readValue   int64
		writeValue  int64
		success     bool
		executionMs int64
	}

	// Create wait group and results channel
	var wg sync.WaitGroup
	resultsCh := make(chan txResult, txCount)

	// Start 4 worker threads, each executing one transaction
	fmt.Fprintf(logFile, "Starting 4 worker threads to execute 4 transactions\n\n")

	for i := 0; i < txCount; i++ {
		wg.Add(1)
		go func(txIndex int) {
			defer wg.Done()

			// Record start time
			startTime := time.Now()

			// Create result object
			result := txResult{
				txIndex:    txIndex,
				writeValue: int64(1000 + txIndex), // Each transaction writes a different value
				success:    true,
			}

			// 1. Read operation
			readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
			if !ok {
				fmt.Fprintf(logFile, "Transaction %d: Read operation failed\n", txIndex)
				result.success = false
				resultsCh <- result
				return
			}

			readHash := readValue.(common.Hash)
			result.readValue = readHash.Big().Int64()

			// Simulate some processing time to create different execution orders
			time.Sleep(time.Duration(10*(txIndex+1)) * time.Millisecond)

			// 2. Write operation
			newValueHash := common.BigToHash(big.NewInt(result.writeValue))
			success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)
			if !success {
				fmt.Fprintf(logFile, "Transaction %d: Write operation failed\n", txIndex)
				result.success = false
			}

			// Record execution time
			result.executionMs = time.Since(startTime).Milliseconds()

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
		fmt.Fprintf(logFile, "Transaction %d:\n", i)
		fmt.Fprintf(logFile, "  Read Value: %d\n", result.readValue)
		fmt.Fprintf(logFile, "  Write Value: %d\n", result.writeValue)
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

	fmt.Fprintf(logFile, "\nTest Completed!\n")
	t.Logf("MVCC concurrent transaction version test completed, detailed results saved in file: mvcc_concurrent_english_test.log")
}
