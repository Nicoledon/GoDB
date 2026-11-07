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
	plan       *planner.NestedLoopJoinNode
	left       Executor
	right      Executor
	count      common.Value
	ctx        *ExecutorContext
	desc       *storage.RawTupleDesc
	pred       storage.Tuple
	init_state bool
	tuples     []storage.Tuple
	index      int
	current    storage.Tuple
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	return &BlockNestedLoopJoinExecutor{
		plan:       plan,
		left:       left,
		right:      right,
		count:      common.NewIntValue(0),
		init_state: true,
		index:      0,
		tuples:     make([]storage.Tuple, 0),
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
	e.init_state = true
	e.index = 0
	e.tuples = make([]storage.Tuple, 0)
	rightdesc := storage.NewRawTupleDesc(e.right.PlanNode().OutputSchema())
	for e.right.Next() {
		e.tuples = append(e.tuples, e.right.Current().DeepCopy(rightdesc))
	}
	if len(e.tuples) == 0 {
		e.init_state = false
	}
	if !e.left.Next() {
		e.init_state = false
	}
	e.current = e.left.Current()
	return nil
}

func (e *BlockNestedLoopJoinExecutor) updateIndex() {
	if e.index == len(e.tuples)-1 {
		e.index = 0
		if !e.left.Next() {
			e.init_state = false
		}
		e.current = e.left.Current()
	} else {
		e.index += 1
	}
}
func (e *BlockNestedLoopJoinExecutor) Next() bool {
	if !e.init_state {
		return false
	}
	bytes := make([]byte, e.desc.BytesPerTuple())
	for e.index < len(e.tuples) {
		tuple := storage.MergeTuples(bytes, e.desc, e.current, e.tuples[e.index])
		pred := e.plan.Predicate.Eval(tuple)
		if pred.IsNull() {
			e.updateIndex()
			if !e.init_state {
				return false
			}
			continue
		}
		if pred.IntValue() == 1 {
			e.pred = tuple
			e.updateIndex()
			return true
		}
		e.updateIndex()
		if !e.init_state {
			return false
		}
	}
	return false
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
