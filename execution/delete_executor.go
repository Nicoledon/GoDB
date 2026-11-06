package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	// Fill me in!
	plan      *planner.DeleteNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	count     common.Value
	ctx       *ExecutorContext
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	return &DeleteExecutor{
		plan:      plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
		count:     common.NewIntValue(0),
	}
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.child.Init(e.ctx)
	e.count = common.NewIntValue(0)
	if err != nil {
		return err
	}
	return nil
}

func (e *DeleteExecutor) Next() bool {
	count := 0
	for {
		next := e.child.Next()
		if !next && count == 0 {
			return false
		} else if !next {
			return true
		}
		count += 1
		tuple := e.child.Current()
		RecordID := tuple.RID()
		err := e.tableHeap.DeleteTuple(e.ctx.txn, RecordID)
		if err != nil {
			return false
		}
		for _, index := range e.indexes {
			rawtuple := make([]byte, 0)
			for _, i := range index.Metadata().ProjectionList {
				temp := make([]byte, tuple.GetValue(i).SizeInBytes())
				tuple.GetValue(i).WriteTo(temp)
				rawtuple = append(rawtuple, temp...)
			}
			key := index.Metadata().AsKey(rawtuple)
			err = index.DeleteEntry(key, RecordID, e.ctx.txn)
			if err != nil {
				panic("entry is not in the index")
			}
		}
		e.count = e.count.Increment()
	}
}

func (e *DeleteExecutor) Current() storage.Tuple {
	tuple := storage.FromValues(e.count)
	return tuple
}

func (e *DeleteExecutor) Close() error {
	return nil
}

func (e *DeleteExecutor) Error() error {
	return nil
}
