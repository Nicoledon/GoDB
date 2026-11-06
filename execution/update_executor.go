package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	plan      *planner.UpdateNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	count     common.Value
	ctx       *ExecutorContext
	// Fill me in!
}

func NewUpdateExecutor(plan *planner.UpdateNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *UpdateExecutor {
	return &UpdateExecutor{
		plan:      plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
		count:     common.NewIntValue(0),
	}
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.child.Init(e.ctx)
	e.count = common.NewIntValue(0)
	if err != nil {
		return err
	}
	return nil
}
func MakeKey(tuple storage.Tuple, index indexing.Index) indexing.Key {
	RawTuple := make([]byte, 0)
	for _, i := range index.Metadata().ProjectionList {
		temp := make([]byte, tuple.GetValue(i).SizeInBytes())
		tuple.GetValue(i).WriteTo(temp)
		RawTuple = append(RawTuple, temp...)
	}
	key := index.Metadata().AsKey(RawTuple)
	return key
}
func (e *UpdateExecutor) Next() bool {
	count := 0
	for {
		next := e.child.Next()
		if !next && count == 0 {
			return false
		} else if !next {
			return true
		}
		count += 1
		OldBuff := make(storage.RawTuple, e.tableHeap.desc.BytesPerTuple())
		_ = e.tableHeap.ReadTuple(e.ctx.txn, e.child.Current().RID(), OldBuff, true)
		OldTuple := storage.FromRawTuple(OldBuff, e.tableHeap.desc, e.child.Current().RID())
		// 把旧的tuple 读出来

		values := make([]common.Value, 0)
		for _, expr := range e.plan.Expressions {
			values = append(values, expr.Eval(e.child.Current()))
		}

		NewBuff := make(storage.RawTuple, e.tableHeap.desc.BytesPerTuple())
		NewTuple := storage.FromValues(values...)
		NewTuple.WriteToBuffer(NewBuff, e.tableHeap.desc)
		_ = e.tableHeap.UpdateTuple(e.ctx.txn, e.child.Current().RID(), NewBuff)

		for _, index := range e.indexes {
			output := make([]common.RecordID, 0)
			OldKey := MakeKey(OldTuple, index)
			NewKey := MakeKey(NewTuple, index)
			output, err := index.ScanKey(OldKey, output, e.ctx.txn)
			if len(output) > 0 && err == nil {
				for _, id := range output {
					err := index.DeleteEntry(OldKey, id, e.ctx.txn)
					if err != nil {
						panic("error in update next")
					}
					err = index.InsertEntry(NewKey, id, e.ctx.txn)
					if err != nil {
						return false
					}
				}
			}
		}
		e.count = e.count.Increment()
	}
}

func (e *UpdateExecutor) OutputSchema() []common.Type {
	panic("unimplemented")
}

func (e *UpdateExecutor) Current() storage.Tuple {
	tuple := storage.FromValues(e.count)
	return tuple
}

func (e *UpdateExecutor) Close() error {
	return nil
}

func (e *UpdateExecutor) Error() error {
	return nil
}
