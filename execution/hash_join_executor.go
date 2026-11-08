package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	// Fill me in!
	plan        *planner.HashJoinNode
	left        Executor
	right       Executor
	ctx         *ExecutorContext
	CacheTuples []storage.Tuple
	hashtable   *ExecutionHashTable[[]storage.Tuple]
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	types := make([]common.Type, 0)
	for _, clause := range plan.RightKeys {
		types = append(types, clause.OutputType())
	}
	return &HashJoinExecutor{
		plan:        plan,
		left:        left,
		right:       right,
		CacheTuples: make([]storage.Tuple, 0),
		hashtable:   NewExecutionHashTable[[]storage.Tuple](storage.NewRawTupleDesc(types)),
	}
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.left.Init(ctx)
	e.right.Init(ctx)
	e.CacheTuples = make([]storage.Tuple, 0)
	types := make([]common.Type, 0)
	for _, clause := range e.plan.RightKeys {
		types = append(types, clause.OutputType())
	}
	e.hashtable = NewExecutionHashTable[[]storage.Tuple](storage.NewRawTupleDesc(types))
	return nil
}

func (e *HashJoinExecutor) Next() bool {
	for e.right.Next() {
		elements := make([]common.Value, 0)
		for _, expr := range e.plan.RightKeys {
			elements = append(elements, expr.Eval(e.right.Current()))
		}
		tuple := storage.FromValues(elements...)
		item, ok := e.hashtable.Get(tuple)
		rightdesc := storage.NewRawTupleDesc(e.right.PlanNode().OutputSchema())
		if ok {
			item = append(item, e.right.Current().DeepCopy(rightdesc))
			e.hashtable.Insert(tuple, item)
		} else {
			temp := make([]storage.Tuple, 0)
			temp = append(temp, e.right.Current().DeepCopy(rightdesc))
			e.hashtable.Insert(tuple, temp)
		}
	}
	if len(e.CacheTuples) != 0 {
		return true
	}
	for e.left.Next() {
		elements := make([]common.Value, 0)
		isnull := false
		for _, expr := range e.plan.LeftKeys {
			val := expr.Eval(e.left.Current())
			if val.IsNull() {
				isnull = true
			}
			elements = append(elements, val)
		}
		if isnull {
			continue
		}
		tuple := storage.FromValues(elements...)
		_, ok := e.hashtable.Get(tuple)
		if !ok {
			continue
		}
		return true
	}
	return false
}

func (e *HashJoinExecutor) Current() storage.Tuple {
	if len(e.CacheTuples) != 0 {
		current := e.CacheTuples[len(e.CacheTuples)-1]
		e.CacheTuples = e.CacheTuples[:len(e.CacheTuples)-1]
		return current
	}
	elements := make([]common.Value, 0)
	for _, expr := range e.plan.LeftKeys {
		elements = append(elements, expr.Eval(e.left.Current()))
	}
	tuple := storage.FromValues(elements...)
	items, ok := e.hashtable.Get(tuple)
	if !ok {
		panic("should not be here")
	}
	for _, item := range items {
		buffer := make([]byte, storage.NewRawTupleDesc(e.plan.OutputSchema()).BytesPerTuple())
		e.CacheTuples = append(e.CacheTuples, storage.MergeTuples(buffer, storage.NewRawTupleDesc(e.plan.OutputSchema()), e.left.Current(), item))
	}
	current := e.CacheTuples[len(e.CacheTuples)-1]
	e.CacheTuples = e.CacheTuples[:len(e.CacheTuples)-1]
	return current
}

func (e *HashJoinExecutor) Error() error {
	return nil
}

func (e *HashJoinExecutor) Close() error {
	return nil
}
