package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	// Fill me in!
	plan  *planner.NestedLoopJoinNode
	left  Executor
	right Executor
	count common.Value
	ctx   *ExecutorContext
	desc  *storage.RawTupleDesc
	pred  storage.Tuple
	cnt   int
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	return &BlockNestedLoopJoinExecutor{
		plan:  plan,
		left:  left,
		right: right,
		count: common.NewIntValue(0),
		cnt:   0,
	}
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.left.Init(ctx)
	e.desc = storage.NewRawTupleDesc(e.plan.OutputSchema())
	if err != nil {
		return err
	}
	err = e.right.Init(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	for {
		if e.cnt == 0 {
			if !e.left.Next() {
				return false
			}
			e.cnt += 1
		}
		if !e.right.Next() {
			e.right.Init(e.ctx)
			if !e.right.Next() {
				return false
			}
			if !e.left.Next() {
				return false
			}
		}
		bytes := make([]byte, e.desc.BytesPerTuple())
		tuple := storage.MergeTuples(bytes, e.desc, e.left.Current(), e.right.Current())
		pred := e.plan.Predicate.Eval(tuple)
		if pred.IntValue() == 1 {
			e.pred = tuple
			return true
		}
	}

}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.pred
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	return nil
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	return nil
}
