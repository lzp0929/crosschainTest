package core

import (
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/mvcc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// 交易执行结果
type txExecutionResult struct {
	index     int
	receipt   *types.Receipt
	gasUsed   uint64
	err       error
	completed bool
}

// BlockParallelProcessor 区块并行处理器
type BlockParallelProcessor struct {
	config *params.ChainConfig
	bc     *BlockChain
	engine consensus.Engine
}

// NewBlockParallelProcessor 创建新的区块并行处理器
func NewBlockParallelProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *BlockParallelProcessor {
	return &BlockParallelProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// ProcessBlock 并行处理区块中的交易 - 使用乐观并发控制
func (p *BlockParallelProcessor) ProcessBlock(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	startTime := time.Now()

	// 准备数据
	header := block.Header()
	txs := block.Transactions()

	// 创建MVCC管理器
	mvccManager := mvcc.NewMVCCStateManager(len(txs))

	// 创建结果通道和结果数组
	resultCh := make(chan *txExecutionResult, len(txs))
	results := make([]*txExecutionResult, len(txs))

	// 设置最大并行度
	maxWorkers := runtime.NumCPU()
	parallelism := maxWorkers
	if len(txs) < maxWorkers {
		parallelism = len(txs)
	}

	// 创建工作池
	var wg sync.WaitGroup
	txCh := make(chan int, len(txs))

	// 添加互斥锁保护状态访问
	var stateLock sync.Mutex

	// 创建重试通道，用于重新执行被中止的交易
	retryCh := make(chan int, len(txs))

	// 创建一个map来跟踪每个交易的重试次数
	retryCount := make(map[int]int)
	var retryLock sync.Mutex

	// 启动工作线程
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				// 优先从重试通道获取交易
				var txIndex int
				var ok bool

				select {
				case txIndex, ok = <-retryCh:
					if !ok {
						// 重试通道已关闭，检查主通道
						txIndex, ok = <-txCh
						if !ok {
							return
						}
					}
				case txIndex, ok = <-txCh:
					if !ok {
						return
					}
				}

				// 获取交易
				tx := txs[txIndex]

				// 创建MVCC状态包装
				mvccState := mvcc.NewMVCCStateDB(statedb, mvccManager, txIndex)

				// 准备交易参数
				gp := new(GasPool).AddGas(block.GasLimit())
				usedGas := uint64(0)

				// 加锁保护状态访问
				stateLock.Lock()
				// 准备状态
				mvccState.Prepare(tx.Hash(), txIndex)
				stateLock.Unlock()

				// 执行交易
				msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
				if err != nil {
					resultCh <- &txExecutionResult{
						index:     txIndex,
						err:       err,
						completed: false,
					}
					continue
				}

				// 创建EVM上下文
				blockContext := NewEVMBlockContext(header, p.bc, nil)
				txContext := NewEVMTxContext(msg)

				// 创建EVM实例并执行交易
				vmenv := vm.NewEVM(blockContext, txContext, mvccState, p.config, cfg)

				// 使用同一流程处理所有交易
				receipt, err := p.applyTransactionWithMVCC(msg, p.config, nil, gp, mvccState, header.Number, header.Hash(), tx, &usedGas, vmenv)

				// 检查交易是否被中止
				if mvccManager.IsTransactionAborted(txIndex) {
					// 增加重试计数
					retryLock.Lock()
					retryCount[txIndex]++
					count := retryCount[txIndex]
					retryLock.Unlock()

					// 如果重试次数未超过限制，将交易重新加入重试队列
					if count < 3 { // 设置最大重试次数为3
						log.Info("交易被中止，准备重试", "txIndex", txIndex, "重试次数", count)
						retryCh <- txIndex
						continue
					}
				}

				resultCh <- &txExecutionResult{
					index:     txIndex,
					receipt:   receipt,
					gasUsed:   usedGas,
					err:       err,
					completed: (err == nil && !mvccManager.IsTransactionAborted(txIndex)),
				}
			}
		}()
	}

	// 按交易索引顺序分派交易
	for i := 0; i < len(txs); i++ {
		txCh <- i
	}
	close(txCh)

	// 等待所有交易处理完成
	go func() {
		wg.Wait()
		close(resultCh)
		close(retryCh)
	}()

	// 收集结果
	for result := range resultCh {
		results[result.index] = result
	}

	// 汇总结果
	totalGas := uint64(0)
	receipts := make([]*types.Receipt, len(txs))
	allLogs := make([]*types.Log, 0)

	for i, result := range results {
		if !result.completed {
			return nil, nil, 0, fmt.Errorf("交易 %d 执行失败", i)
		}

		receipts[i] = result.receipt
		totalGas += result.gasUsed
		allLogs = append(allLogs, result.receipt.Logs...)
	}

	// 处理区块奖励
	p.engine.Finalize(p.bc, header, statedb, txs, block.Uncles())

	log.Info("区块并行执行完成",
		"区块", block.Number(),
		"交易总数", len(txs),
		"耗时", time.Since(startTime))

	return receipts, allLogs, totalGas, nil
}

// 串行应用交易到状态
func (p *BlockParallelProcessor) applyTransactionToState(tx *types.Transaction, header *types.Header, statedb *state.StateDB, txIndex int, cfg vm.Config) (*types.Receipt, uint64, error) {
	msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
	if err != nil {
		return nil, 0, err
	}

	// 准备状态
	statedb.Prepare(tx.Hash(), txIndex)

	// 创建EVM上下文
	gp := new(GasPool).AddGas(header.GasLimit)
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	txContext := NewEVMTxContext(msg)
	vmenv := vm.NewEVM(blockContext, txContext, statedb, p.config, cfg)

	// 执行交易
	gasUsed := uint64(0)
	receipt, err := applyTransaction(msg, p.config, nil, gp, statedb, header.Number, header.Hash(), tx, &gasUsed, vmenv)

	return receipt, gasUsed, err
}

// applyTransactionWithMVCC 使用MVCC状态处理交易
func (p *BlockParallelProcessor) applyTransactionWithMVCC(
	msg types.Message,
	config *params.ChainConfig,
	bc ChainContext,
	gp *GasPool,
	mvccState *mvcc.MVCCStateDB,
	blockNumber *big.Int,
	blockHash common.Hash,
	tx *types.Transaction,
	usedGas *uint64,
	evm *vm.EVM) (*types.Receipt, error) {

	// 判断是否为合约调用
	isContractCall := tx.To() != nil && len(tx.Data()) > 0

	if isContractCall {
		// 记录智能合约调用
		log.Debug("执行智能合约交易", "txIndex", tx.Nonce(), "to", tx.To())
	}

	return applyTransaction(msg, config, nil, gp, mvccState.GetBaseState(), blockNumber, blockHash, tx, usedGas, evm)
}
