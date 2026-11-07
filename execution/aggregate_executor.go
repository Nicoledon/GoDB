package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	// Fill me in!
	ctx            *ExecutorContext
	plan           *planner.AggregateNode
	child          Executor
	hashtable      *ExecutionHashTable[[]common.Value]
	cnt            int
	tuples         []storage.Tuple
	keys           []storage.Tuple
	current        storage.Tuple
	usedgroupby    bool
	count          int64
	notusedgroupby []common.Value
	over           bool
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	types := make([]common.Type, 0)
	for _, groupclause := range plan.GroupByClause {
		types = append(types, groupclause.OutputType())
	}

	return &AggregateExecutor{
		plan:           plan,
		child:          child,
		hashtable:      NewExecutionHashTable[[]common.Value](storage.NewRawTupleDesc(types)),
		cnt:            0,
		tuples:         make([]storage.Tuple, 0),
		keys:           make([]storage.Tuple, 0),
		usedgroupby:    true,
		count:          0,
		notusedgroupby: make([]common.Value, 0),
		over:           false,
	}
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.child.Init(ctx)
	return nil
}
func EvalExpr(tuple storage.Tuple, clause planner.AggregateClause, prev common.Value) common.Value {
	a := clause.Type
	value := clause.Expr.Eval(tuple)
	if value.IsNull() {
		return prev
	}
	if prev.IsNull() {
		return value
	}
	switch a {
	case 0:
		return prev.Increment()
	case 1:
		return common.NewIntValue(prev.IntValue() + value.IntValue())
	case 2:
		if prev.Compare(value) == 1 {
			return value
		}
		return prev
	case 3:
		if prev.Compare(value) == -1 {
			return value
		}
		return prev
	}
	return common.Value{}
}
func (e *AggregateExecutor) handtypes(key storage.Tuple, tuple storage.Tuple) {
	item, ok := e.hashtable.Get(key)
	if !ok {
		e.keys = append(e.keys, key)
		container := make([]common.Value, 0)
		for _, aggclause := range e.plan.AggClauses {
			if aggclause.Type == 0 {
				container = append(container, common.NewIntValue(1))
			} else {
				container = append(container, aggclause.Expr.Eval(tuple))
			}
		}
		e.hashtable.Insert(key, container)
	} else {
		container := make([]common.Value, 0)
		for i, aggclause := range e.plan.AggClauses {
			container = append(container, EvalExpr(tuple, aggclause, item[i]))
		}
		e.hashtable.Insert(key, container)
	}
}
func (e *AggregateExecutor) handnull(clause planner.AggregateClause, tuple storage.Tuple) bool {
	value := clause.Expr.Eval(tuple)
	if value.IsNull() {
		return true
	}
	return false
}
func (e *AggregateExecutor) handAlltypes(key storage.Tuple, tuple storage.Tuple) {
	if e.count == 0 {
		for _, aggclause := range e.plan.AggClauses {
			if aggclause.Type == 0 {
				if e.handnull(aggclause, tuple) {
					e.notusedgroupby = append(e.notusedgroupby, common.NewNullInt())
				} else {
					e.notusedgroupby = append(e.notusedgroupby, common.NewIntValue(1))
				}
			} else {
				if e.handnull(aggclause, tuple) {
					e.notusedgroupby = append(e.notusedgroupby, common.NewNullInt())
				} else {
					e.notusedgroupby = append(e.notusedgroupby, aggclause.Expr.Eval(tuple))
				}
			}
		}
		e.count += 1
	} else {
		for i, aggclause := range e.plan.AggClauses {
			if e.notusedgroupby[i].IsNull() {
				continue
			} else {
				e.notusedgroupby[i] = EvalExpr(tuple, aggclause, e.notusedgroupby[i])
			}
		}
	}
}
func (e *AggregateExecutor) Next() bool {
	if e.over {
		return false
	}
	if e.cnt == 0 {
		for e.child.Next() {
			e.tuples = append(e.tuples, e.child.Current().DeepCopy(storage.NewRawTupleDesc(e.child.PlanNode().OutputSchema())))
		}
		for _, tuple := range e.tuples {
			values := make([]common.Value, 0)
			for _, groupclause := range e.plan.GroupByClause {
				values = append(values, groupclause.Eval(tuple))
			}
			key := storage.FromValues(values...)
			if len(values) == 0 {
				e.usedgroupby = false
				e.handAlltypes(key, tuple)
			} else {
				e.handtypes(key, tuple)
			}
			e.cnt += 1
		}
	}
	if !e.usedgroupby {
		if len(e.notusedgroupby) != 0 {
			e.over = true
			return true
		} else {
			return false
		}
	} else {
		if len(e.keys) == 0 {
			return false
		} else {
			e.current = e.keys[0]
			e.keys = e.keys[1:]
		}
		return true
	}
}

func (e *AggregateExecutor) Current() storage.Tuple {
	if !e.usedgroupby {
		tuple := storage.FromValues(e.notusedgroupby...)
		return tuple
	} else {
		item, ok := e.hashtable.Get(e.current)
		key := make([]common.Value, 0)
		if !ok {
			panic("error")
		} else {
			for i := 0; i < e.current.NumColumns(); i++ {
				key = append(key, e.current.GetValue(i))
			}
			for _, i := range item {
				key = append(key, i)
			}
			return storage.FromValues(key...)
		}
	}
}

func (e *AggregateExecutor) Error() error {
	return nil
}

func (e *AggregateExecutor) Close() error {
	return nil
}
