package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	// Fill me in!
	ctx     *ExecutorContext
	plan    *planner.SortNode
	child   Executor
	tuples  []storage.Tuple
	result  []SortMetaData
	cnt     int
	current storage.Tuple
}
type SortMetaData struct {
	tuple     storage.Tuple
	field     []common.Value
	Direction []planner.SortDirection
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	return &SortExecutor{
		plan:   plan,
		child:  child,
		tuples: make([]storage.Tuple, 0),
	}
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.child.Init(ctx)
	return nil
}
func Compare(a SortMetaData, b SortMetaData) bool {
	for i := 0; i < len(a.field); i++ {
		if a.Direction[i] == 0 {
			if a.field[i].Compare(b.field[i]) == 1 {
				return true
			} else if a.field[i].Compare(b.field[i]) == -1 {
				return false
			} else {
				continue
			}
		} else {
			if a.field[i].Compare(b.field[i]) == -1 {
				return true
			} else if a.field[i].Compare(b.field[i]) == 1 {
				return false
			} else {
				continue
			}
		}
	}
	return false
}
func BubbleSort(Sorts []SortMetaData) {
	n := len(Sorts)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-1-i; j++ {
			if Compare(Sorts[j], Sorts[j+1]) {
				Sorts[j], Sorts[j+1] = Sorts[j+1], Sorts[j]
			}
		}
	}
}
func (e *SortExecutor) Next() bool {
	if e.cnt == 0 {
		for e.child.Next() {
			e.tuples = append(e.tuples, e.child.Current().DeepCopy(storage.NewRawTupleDesc(e.child.PlanNode().OutputSchema())))
		}
		if len(e.tuples) == 0 {
			return false
		}
		Sorts := make([]SortMetaData, 0)
		for _, item := range e.tuples {
			values := make([]common.Value, 0)
			directs := make([]planner.SortDirection, 0)
			for _, expr := range e.plan.OrderBy {
				values = append(values, expr.Expr.Eval(item))
				directs = append(directs, expr.Direction)
			}
			Sorts = append(Sorts, SortMetaData{item, values, directs})
		}
		BubbleSort(Sorts)
		e.result = Sorts
		e.cnt += 1
		e.current = e.result[0].tuple
		e.result = e.result[1:]
		return true
	}
	if len(e.result) == 0 {
		return false
	}
	e.current = e.result[0].tuple
	e.result = e.result[1:]
	return true
}

func (e *SortExecutor) Current() storage.Tuple {
	return e.current
}

func (e *SortExecutor) Error() error {
	return nil
}

func (e *SortExecutor) Close() error {
	return nil
}
