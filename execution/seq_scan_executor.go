package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	// Fill me in!
	plan      *planner.SeqScanNode
	tableHeap *TableHeap
	iter      *TableHeapIterator
	ctx       *ExecutorContext
	tuple     storage.RawTuple
	err       error
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	return &SeqScanExecutor{
		plan:      plan,
		tableHeap: tableHeap,
		err:       nil,
	}
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {

	return e.plan
}

func (e *SeqScanExecutor) Init(context *ExecutorContext) error {
	e.ctx = context
	item, err := e.tableHeap.Iterator(e.ctx.txn, e.plan.Mode, make([]byte, e.tableHeap.desc.BytesPerTuple()))
	if err != nil {
		e.err = err
		return err
	}
	e.iter = &item
	return nil
}

func (e *SeqScanExecutor) Next() bool {
	if e.iter.Next() {
		e.tuple = e.iter.CurrentTuple()
		return true
	}
	return false
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.tuple, e.tableHeap.desc, e.iter.CurrentRID())
}

func (e *SeqScanExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if e.iter != nil {
		return e.iter.Error()
	}
	return nil
}

func (e *SeqScanExecutor) Close() error {
	err := e.iter.Close()
	if err != nil {
		return err
	}
	return nil
}
