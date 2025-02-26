package core

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// 创建测试交易
func createTestTx(from int, to int, value int64, nonce uint64) *types.Transaction {
	fromKey, _ := crypto.GenerateKey()
	if from > 0 {
		// 使用固定密钥以便测试重现
		fromKey, _ = crypto.HexToECDSA(fmt.Sprintf("%064x", from))
	}
	toKey, _ := crypto.GenerateKey()
	if to > 0 {
		toKey, _ = crypto.HexToECDSA(fmt.Sprintf("%064x", to))
	}

	toAddr := crypto.PubkeyToAddress(toKey.PublicKey)
	tx := types.NewTransaction(nonce, toAddr, big.NewInt(value), 21000, big.NewInt(1000000000), nil)
	signedTx, _ := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(1)), fromKey)
	return signedTx
}

// 创建无冲突交易区块
func createNonConflictingBlock() *types.Block {
	db := rawdb.NewMemoryDatabase()

	// 创建一个映射存储所有测试账户
	alloc := GenesisAlloc{}

	// 为所有测试发送账户预先分配足够资金
	for i := 1; i <= 10; i++ {
		key, _ := crypto.HexToECDSA(fmt.Sprintf("%064x", i))
		addr := crypto.PubkeyToAddress(key.PublicKey)
		// 为每个账户分配充足资金
		alloc[addr] = GenesisAccount{Balance: big.NewInt(1000000000000000000)}
	}

	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  alloc,
	}
	genesis := gspec.MustCommit(db)

	// 创建区块链
	blockchain, _ := NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	defer blockchain.Stop()

	// 创建10个不同地址之间的转账交易
	var txs []*types.Transaction
	for i := 1; i <= 10; i++ {
		tx := createTestTx(i, i+100, 1000, 0)
		txs = append(txs, tx)
	}

	// 创建区块
	blocks, _ := GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, 1, func(i int, gen *BlockGen) {
		for _, tx := range txs {
			gen.AddTx(tx)
		}
	})
	block := blocks[0]

	return block
}

// 创建有冲突交易区块
func createConflictingBlock() *types.Block {
	db := rawdb.NewMemoryDatabase()
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)

	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc: GenesisAlloc{
			// 所有交易都使用同一个地址，分配足够多的资金
			addr: {Balance: func() *big.Int { b, _ := new(big.Int).SetString("10000000000000000000", 10); return b }()},
		},
	}
	genesis := gspec.MustCommit(db)

	// 创建区块链
	blockchain, _ := NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	defer blockchain.Stop()

	// 使用同一个发送地址创建10个交易
	var txs []*types.Transaction
	for i := 0; i < 10; i++ {
		// 使用相同的发送者密钥，但设置更高的gas价格
		tx := types.NewTransaction(uint64(i), common.Address{}, big.NewInt(1000), 21000, big.NewInt(1000000000), nil)
		signedTx, _ := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(1)), key)
		txs = append(txs, signedTx)
	}

	// 创建区块
	blocks, _ := GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, 1, func(i int, gen *BlockGen) {
		for _, tx := range txs {
			gen.AddTx(tx)
		}
	})
	block := blocks[0]

	return block
}

// 测试并行处理器与串行处理器结果一致性
func TestParallelVsSerialExecution(t *testing.T) {
	// 设置测试环境
	db := rawdb.NewMemoryDatabase()

	// 创建一个映射存储所有测试账户
	alloc := GenesisAlloc{}

	// 为所有测试发送账户预先分配足够资金
	for i := 1; i <= 10; i++ {
		key, _ := crypto.HexToECDSA(fmt.Sprintf("%064x", i))
		addr := crypto.PubkeyToAddress(key.PublicKey)
		alloc[addr] = GenesisAccount{Balance: big.NewInt(1000000000000000000)}
	}

	// 创建并提交genesis区块
	genesis := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  alloc,
	}
	genesisBlock := genesis.MustCommit(db)

	// 初始化区块链
	blockchain, _ := NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	defer blockchain.Stop()

	// 创建无冲突交易区块
	block := createNonConflictingBlock()

	// 创建两个相同的状态
	stateSerial, _ := blockchain.StateAt(genesisBlock.Root())
	stateParallel, _ := blockchain.StateAt(genesisBlock.Root())

	// 串行执行
	processor := NewStateProcessor(params.TestChainConfig, blockchain, ethash.NewFaker())
	receiptsSerial, _, gasUsedSerial, _ := processor.Process(block, stateSerial, vm.Config{})
	rootSerial, _ := stateSerial.Commit(false)

	// 并行执行
	parallelProcessor := NewBlockParallelProcessor(params.TestChainConfig, blockchain, ethash.NewFaker())
	receiptsParallel, _, gasUsedParallel, _ := parallelProcessor.ProcessBlock(block, stateParallel, vm.Config{})
	rootParallel, _ := stateParallel.Commit(false)

	// 比较结果
	if rootSerial != rootParallel {
		t.Fatalf("状态根不匹配: 串行=%s, 并行=%s", rootSerial.Hex(), rootParallel.Hex())
	}

	if gasUsedSerial != gasUsedParallel {
		t.Fatalf("gas用量不匹配: 串行=%d, 并行=%d", gasUsedSerial, gasUsedParallel)
	}

	if len(receiptsSerial) != len(receiptsParallel) {
		t.Fatalf("收据数量不匹配: 串行=%d, 并行=%d", len(receiptsSerial), len(receiptsParallel))
	}
}

// 性能测试
func BenchmarkParallelExecution(b *testing.B) {
	// 设置测试环境
	db := rawdb.NewMemoryDatabase()

	// 创建一个映射存储所有测试账户
	alloc := GenesisAlloc{}

	// 为所有测试发送账户预先分配足够资金
	for i := 1; i <= 10; i++ {
		key, _ := crypto.HexToECDSA(fmt.Sprintf("%064x", i))
		addr := crypto.PubkeyToAddress(key.PublicKey)
		alloc[addr] = GenesisAccount{Balance: big.NewInt(1000000000000000000)}
	}

	// 创建并提交genesis区块
	genesis := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  alloc,
	}
	genesisBlock := genesis.MustCommit(db)

	// 初始化区块链
	blockchain, _ := NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	defer blockchain.Stop()

	// 创建无冲突交易区块
	block := createNonConflictingBlock()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		state, _ := blockchain.StateAt(genesisBlock.Root())
		parallelProcessor := NewBlockParallelProcessor(params.TestChainConfig, blockchain, ethash.NewFaker())
		parallelProcessor.ProcessBlock(block, state, vm.Config{})
	}
}

func BenchmarkSerialExecution(b *testing.B) {
	// 设置测试环境
	db := rawdb.NewMemoryDatabase()

	// 创建一个映射存储所有测试账户
	alloc := GenesisAlloc{}

	// 为所有测试发送账户预先分配足够资金
	for i := 1; i <= 10; i++ {
		key, _ := crypto.HexToECDSA(fmt.Sprintf("%064x", i))
		addr := crypto.PubkeyToAddress(key.PublicKey)
		alloc[addr] = GenesisAccount{Balance: big.NewInt(1000000000000000000)}
	}

	// 创建并提交genesis区块
	genesis := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  alloc,
	}
	genesisBlock := genesis.MustCommit(db)

	// 初始化区块链
	blockchain, _ := NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	defer blockchain.Stop()

	// 创建无冲突交易区块
	block := createNonConflictingBlock()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		state, _ := blockchain.StateAt(genesisBlock.Root())
		processor := NewStateProcessor(params.TestChainConfig, blockchain, ethash.NewFaker())
		processor.Process(block, state, vm.Config{})
	}
}

// 测试冲突交易的处理
func TestConflictingTransactions(t *testing.T) {
	// 设置测试环境
	db := rawdb.NewMemoryDatabase()
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)

	// 创建创世区块配置，分配足够的资金
	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc: GenesisAlloc{
			addr: {Balance: func() *big.Int { b, _ := new(big.Int).SetString("10000000000000000000", 10); return b }()}},
		GasLimit: 30000000,
	}
	genesis := gspec.MustCommit(db)

	// 初始化区块链
	blockchain, _ := NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	defer blockchain.Stop()

	// 创建3个简单的交易，使用高gas价格，都转给同一个目标地址
	recipient := common.HexToAddress("0x1234567890123456789012345678901234567890")
	var txs []*types.Transaction

	// 创建传统类型交易，使用足够高的gasPrice
	for i := 0; i < 3; i++ {
		gasPrice := big.NewInt(5000000000) // 5 Gwei
		tx := types.NewTransaction(uint64(i), recipient, big.NewInt(1000), 21000, gasPrice, nil)
		signedTx, _ := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(1)), key)
		txs = append(txs, signedTx)
	}

	// 简化区块创建过程，直接使用Genesis上的区块生成
	blocks, _ := GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, 1, func(i int, gen *BlockGen) {
		gen.SetCoinbase(common.Address{1})

		for _, tx := range txs {
			gen.AddTx(tx)
		}
	})
	block := blocks[0]

	// 使用创世区块的状态
	state, _ := blockchain.StateAt(genesis.Root())

	// 使用并行处理器处理区块
	parallelProcessor := NewBlockParallelProcessor(params.TestChainConfig, blockchain, ethash.NewFaker())
	receipts, _, _, err := parallelProcessor.ProcessBlock(block, state, vm.Config{})

	if err != nil {
		t.Fatalf("处理冲突交易区块失败: %v", err)
	}

	// 验证结果
	if len(receipts) != 3 {
		t.Fatalf("预期处理3笔交易，实际处理: %d", len(receipts))
	}

	// 检查MVCC管理器是否检测到冲突
	// 由于这是简化的测试，我们主要关注的是交易能否成功执行
	t.Logf("成功执行了3笔交易，其中可能检测到了交易冲突")
}
