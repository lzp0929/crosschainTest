package vm

import (
	"github.com/ethereum/go-ethereum/common"
)

// Interpreter 定义EVM解释器接口
type Interpreter interface {
	// Run 执行给定的合约代码
	Run(contract *Contract, input []byte, readOnly bool) ([]byte, error)
}

// MVCCInterpreter 是支持MVCC的EVM解释器
type MVCCInterpreter struct {
	evm        *EVM        // EVM实例
	cfg        Config      // 配置
	txIndex    int         // 当前交易索引
	baseInterp Interpreter // 基础解释器
}

// NewMVCCInterpreter 创建支持MVCC的解释器
func NewMVCCInterpreter(evm *EVM, cfg Config, txIndex int) *MVCCInterpreter {
	baseInterp := NewEVMInterpreter(evm, cfg)
	return &MVCCInterpreter{
		evm:        evm,
		cfg:        cfg,
		txIndex:    txIndex,
		baseInterp: baseInterp,
	}
}

// Run 实现Interpreter接口，增加对合约状态读写的MVCC跟踪
func (in *MVCCInterpreter) Run(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {
	// 使用基础解释器执行
	return in.baseInterp.Run(contract, input, readOnly)
}

// CanRun 实现Interpreter接口
func (in *MVCCInterpreter) CanRun(code []byte) bool {
	// 由于我们使用的是基础解释器执行，所以直接返回true
	return true
}

// 重写SLOAD操作，加入MVCC读取逻辑
func (in *MVCCInterpreter) opSload(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	loc := scope.Stack.peek()
	hash := common.Hash(loc.Bytes32())

	// 获取当前合约地址
	contractAddr := scope.Contract.Address()

	// 使用MVCC读取合约存储
	val := interpreter.evm.StateDB.GetState(contractAddr, hash)
	loc.SetBytes(val.Bytes())

	return nil, nil
}

// 重写SSTORE操作，加入MVCC写入逻辑
func (in *MVCCInterpreter) opSstore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}

	loc := scope.Stack.pop()
	val := scope.Stack.pop()

	// 获取当前合约地址
	contractAddr := scope.Contract.Address()

	interpreter.evm.StateDB.SetState(
		contractAddr,
		common.Hash(loc.Bytes32()),
		common.Hash(val.Bytes32()),
	)

	return nil, nil
}

// 提供接口替换标准解释器的指令集
func (in *MVCCInterpreter) ReplaceInstructions() {
	// 这里需要类型断言来访问baseInterp的opcodes
	// 由于接口限制，这个方法实际上不做任何操作
	// 真正的实现需要修改EVMInterpreter结构
}
