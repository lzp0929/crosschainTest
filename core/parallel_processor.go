package core

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/mvcc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

type ParallelProcessor struct {
	bc       *BlockChain
	config   *params.ChainConfig
	vmConfig vm.Config
	statedb  *state.StateDB
	txs      []*types.Transaction
	receipts []*types.Receipt
	header   *types.Header
	author   *common.Address
	gp       *GasPool
	mu       sync.Mutex
}

func NewBlockParallelProcessor(bc *BlockChain, config *params.ChainConfig, vmConfig vm.Config, statedb *state.StateDB, header *types.Header, author *common.Address, gp *GasPool, txs []*types.Transaction) *ParallelProcessor {
	return &ParallelProcessor{
		bc:       bc,
		config:   config,
		vmConfig: vmConfig,
		statedb:  statedb,
		txs:      txs,
		receipts: make([]*types.Receipt, len(txs)),
		header:   header,
		author:   author,
		gp:       gp,
	}
}

func (p *ParallelProcessor) Process() ([]*types.Receipt, error) {
	var wg sync.WaitGroup
	errCh := make(chan error, len(p.txs))

	// 并行处理交易
	for i := 0; i < len(p.txs); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// 创建MVCC状态包装器
			mvccState := mvcc.NewMVCCStateDB(p.statedb, index)

			// 处理交易
			receipt, err := p.processTransaction(p.txs[index], mvccState)
			if err != nil {
				errCh <- err
				return
			}

			// 保存收据
			p.mu.Lock()
			p.receipts[index] = receipt
			p.mu.Unlock()
		}(i)
	}

	// 等待所有交易处理完成
	wg.Wait()
	close(errCh)

	// 检查是否有错误
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	return p.receipts, nil
}

func (p *ParallelProcessor) processTransaction(tx *types.Transaction, mvccState *mvcc.MVCCStateDB) (*types.Receipt, error) {
	// 应用交易
	usedGas := uint64(0)
	receipt, err := ApplyTransactionWithMVCC(p.config, p.bc, p.author, p.gp, mvccState, p.header, tx, &usedGas, p.vmConfig)
	if err != nil {
		return nil, err
	}

	return receipt, nil
}

func ApplyTransactionWithMVCC(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *mvcc.MVCCStateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	// 获取MVCC管理器
	mvccManager := statedb.GetMVCCManager()
	txIndex := statedb.GetTxIndex()

	// 使用MVCC执行交易
	var receipt *types.Receipt

	execErr := mvccManager.ExecuteTransaction(txIndex, func() error {
		// 将交易转换为消息
		msg, err := tx.AsMessage(types.MakeSigner(config, header.Number), header.BaseFee)
		if err != nil {
			return err
		}

		// 创建EVM上下文
		blockContext := NewEVMBlockContext(header, bc, author)
		txContext := vm.NewTxContext(msg.From(), tx.Hash(), header.Number.Uint64(), header.Time, header.BaseFee)
		vmenv := vm.NewEVM(blockContext, txContext, statedb.GetBaseState(), config, cfg)

		// 执行消息
		result, err := ApplyMessage(vmenv, msg, gp)
		if err != nil {
			return err
		}

		// 更新已用Gas
		*usedGas += result.UsedGas

		// 创建收据
		var root []byte
		if config.IsByzantium(header.Number) {
			statedb.GetBaseState().Finalise(true)
		} else {
			root = statedb.GetBaseState().IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
		}

		receipt = types.NewReceipt(root, result.Failed(), *usedGas)
		receipt.TxHash = tx.Hash()
		receipt.GasUsed = result.UsedGas

		// 如果是合约创建交易，存储创建的合约地址
		if msg.To() == nil {
			receipt.ContractAddress = crypto.CreateAddress(msg.From(), tx.Nonce())
		}

		// 设置日志和过滤器
		receipt.Logs = statedb.GetBaseState().GetLogs(tx.Hash(), header.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockHash = header.Hash()
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(txIndex)

		return nil
	})

	if execErr != nil {
		return nil, execErr
	}

	return receipt, nil
}
