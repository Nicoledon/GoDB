package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// InsertExecutor executes an INSERT query.
// It consumes tuples from its child (which could be a ValuesExecutor or a SELECT query),
// inserts them into the TableHeap, and updates all associated indexes.
//
// For this course, you may assume that the child does not read from the table you are inserting into
type InsertExecutor struct {
	// Fill me in!
	plan      *planner.InsertNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	count     common.Value
	ctx       *ExecutorContext
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	return &InsertExecutor{
		plan:      plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
		count:     common.NewIntValue(0),
	}
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.child.Init(e.ctx)
	e.count = common.NewIntValue(0)
	if err != nil {
		return err
	}
	return nil
}

func (e *InsertExecutor) Next() bool {
	count := 0
	for {
		next := e.child.Next()
		if !next && count == 0 {
			return false
		} else if !next {
			return true
		}
		count += 1
		var buffer storage.RawTuple = make(storage.RawTuple, e.tableHeap.desc.BytesPerTuple())

		e.child.Current().WriteToBuffer(buffer, e.tableHeap.desc)
		RecordID, err := e.tableHeap.InsertTuple(e.ctx.txn, buffer)
		tuple := storage.FromRawTuple(buffer, e.tableHeap.desc, RecordID)
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
			//println("Insert RID: %v, Key Bytes: %x\n", RecordID.PageNum, key.RawTuple)
			err = index.InsertEntry(key, RecordID, e.ctx.txn)
			if err != nil {
				panic("insert to wrong index")
			}
		}
		e.count = e.count.Increment()
	}
}

func (e *InsertExecutor) Current() storage.Tuple {
	tuple := storage.FromValues(e.count)
	return tuple
}

func (e *InsertExecutor) Close() error {
	return nil
}

func (e *InsertExecutor) Error() error {
	return nil
}
