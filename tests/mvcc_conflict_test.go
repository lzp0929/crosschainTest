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

// TestMVCCConflictDetection tests the MVCC conflict detection and transaction abort mechanism
func TestMVCCConflictDetection(t *testing.T) {
	// Create log file
	logFile, err := os.Create("mvcc_conflict_test.log")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()

	// Write log header
	fmt.Fprintf(logFile, "MVCC Conflict Detection Test\n")
	fmt.Fprintf(logFile, "==========================\n\n")

	// Create memory database and state DB for testing
	db := rawdb.NewMemoryDatabase()
	stateDB, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)

	// Create MVCC manager for 5 transactions
	txCount := 5
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
		aborted     bool
	}

	// Create wait group and results channel
	var wg sync.WaitGroup
	resultsCh := make(chan txResult, txCount)

	// Execution plan:
	// - Tx0: Read, wait, write (value=1000)
	// - Tx1: Read, write (value=1001), commit quickly
	// - Tx2: Read after Tx1 writes, write (value=1002)
	// - Tx3: Try to read Tx0's write (should be aborted due to conflict)
	// - Tx4: Read after Tx2 writes, write (value=1004)

	fmt.Fprintf(logFile, "Starting 5 worker threads with specific execution order to create conflicts\n\n")

	// Transaction 0 - will be slow and create conflict
	wg.Add(1)
	go func() {
		defer wg.Done()
		txIndex := 0

		// Record start time
		startTime := time.Now()

		// Create result object
		result := txResult{
			txIndex:    txIndex,
			writeValue: 1000,
			success:    true,
		}

		// 1. Read operation
		readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
		if !ok {
			fmt.Fprintf(logFile, "Transaction %d: Read operation failed\n", txIndex)
			result.success = false
			result.aborted = true
			resultsCh <- result
			return
		}

		readHash := readValue.(common.Hash)
		result.readValue = readHash.Big().Int64()

		// Sleep to allow Tx1 to write before this transaction
		time.Sleep(50 * time.Millisecond)

		// 2. Write operation - this should fail due to conflict with Tx1
		newValueHash := common.BigToHash(big.NewInt(result.writeValue))
		success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)

		result.success = success
		if !success {
			fmt.Fprintf(logFile, "Transaction %d: Write operation failed (expected due to conflict)\n", txIndex)
			result.aborted = true
		}

		// Record execution time
		result.executionMs = time.Since(startTime).Milliseconds()

		// Send result
		resultsCh <- result
	}()

	// Transaction 1 - will execute quickly and commit
	wg.Add(1)
	go func() {
		defer wg.Done()
		txIndex := 1

		// Record start time
		startTime := time.Now()

		// Create result object
		result := txResult{
			txIndex:    txIndex,
			writeValue: 1001,
			success:    true,
		}

		// Sleep a tiny bit to ensure Tx0 reads first
		time.Sleep(5 * time.Millisecond)

		// 1. Read operation
		readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
		if !ok {
			fmt.Fprintf(logFile, "Transaction %d: Read operation failed\n", txIndex)
			result.success = false
			result.aborted = true
			resultsCh <- result
			return
		}

		readHash := readValue.(common.Hash)
		result.readValue = readHash.Big().Int64()

		// 2. Write operation - should succeed
		newValueHash := common.BigToHash(big.NewInt(result.writeValue))
		success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)

		result.success = success
		if !success {
			fmt.Fprintf(logFile, "Transaction %d: Write operation failed unexpectedly\n", txIndex)
			result.aborted = true
		}

		// Record execution time
		result.executionMs = time.Since(startTime).Milliseconds()

		// Send result
		resultsCh <- result
	}()

	// Transaction 2 - will read after Tx1 writes
	wg.Add(1)
	go func() {
		defer wg.Done()
		txIndex := 2

		// Record start time
		startTime := time.Now()

		// Create result object
		result := txResult{
			txIndex:    txIndex,
			writeValue: 1002,
			success:    true,
		}

		// Sleep to ensure Tx1 writes before this transaction reads
		time.Sleep(20 * time.Millisecond)

		// 1. Read operation - should read Tx1's write
		readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
		if !ok {
			fmt.Fprintf(logFile, "Transaction %d: Read operation failed\n", txIndex)
			result.success = false
			result.aborted = true
			resultsCh <- result
			return
		}

		readHash := readValue.(common.Hash)
		result.readValue = readHash.Big().Int64()

		// 2. Write operation - should succeed
		newValueHash := common.BigToHash(big.NewInt(result.writeValue))
		success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)

		result.success = success
		if !success {
			fmt.Fprintf(logFile, "Transaction %d: Write operation failed unexpectedly\n", txIndex)
			result.aborted = true
		}

		// Record execution time
		result.executionMs = time.Since(startTime).Milliseconds()

		// Send result
		resultsCh <- result
	}()

	// Transaction 3 - will try to read Tx0's write (which will be aborted)
	wg.Add(1)
	go func() {
		defer wg.Done()
		txIndex := 3

		// Record start time
		startTime := time.Now()

		// Create result object
		result := txResult{
			txIndex:    txIndex,
			writeValue: 1003,
			success:    true,
		}

		// Sleep to ensure Tx0 attempts to write before this transaction reads
		time.Sleep(60 * time.Millisecond)

		// 1. Read operation - should not be able to read Tx0's write (it should be aborted)
		readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
		if !ok {
			fmt.Fprintf(logFile, "Transaction %d: Read operation failed\n", txIndex)
			result.success = false
			result.aborted = true
			resultsCh <- result
			return
		}

		readHash := readValue.(common.Hash)
		result.readValue = readHash.Big().Int64()

		// 2. Write operation
		newValueHash := common.BigToHash(big.NewInt(result.writeValue))
		success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)

		result.success = success
		if !success {
			fmt.Fprintf(logFile, "Transaction %d: Write operation failed\n", txIndex)
			result.aborted = true
		}

		// Record execution time
		result.executionMs = time.Since(startTime).Milliseconds()

		// Send result
		resultsCh <- result
	}()

	// Transaction 4 - will read after Tx2 writes
	wg.Add(1)
	go func() {
		defer wg.Done()
		txIndex := 4

		// Record start time
		startTime := time.Now()

		// Create result object
		result := txResult{
			txIndex:    txIndex,
			writeValue: 1004,
			success:    true,
		}

		// Sleep to ensure Tx2 writes before this transaction reads
		time.Sleep(40 * time.Millisecond)

		// 1. Read operation - should read Tx2's write
		readValue, ok := mvccManager.ReadStorageState(txIndex, contractAddr, storageKey, stateDB)
		if !ok {
			fmt.Fprintf(logFile, "Transaction %d: Read operation failed\n", txIndex)
			result.success = false
			result.aborted = true
			resultsCh <- result
			return
		}

		readHash := readValue.(common.Hash)
		result.readValue = readHash.Big().Int64()

		// 2. Write operation - should succeed
		newValueHash := common.BigToHash(big.NewInt(result.writeValue))
		success := mvccManager.WriteStorageState(txIndex, contractAddr, storageKey, newValueHash)

		result.success = success
		if !success {
			fmt.Fprintf(logFile, "Transaction %d: Write operation failed unexpectedly\n", txIndex)
			result.aborted = true
		}

		// Record execution time
		result.executionMs = time.Since(startTime).Milliseconds()

		// Send result
		resultsCh <- result
	}()

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
		fmt.Fprintf(logFile, "  Aborted: %v\n", result.aborted)
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

	// Verify that at least one transaction was aborted (Tx0 should be aborted)
	if len(abortedTxs) == 0 {
		t.Errorf("Expected at least one transaction to be aborted, but none were")
	}

	fmt.Fprintf(logFile, "\nTest Completed!\n")
	t.Logf("MVCC conflict detection test completed, detailed results saved in file: mvcc_conflict_test.log")
}
