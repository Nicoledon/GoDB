package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	// Fill me in!
	plan      *planner.IndexScanNode
	index     indexing.Index
	tableHeap *TableHeap
	iter      indexing.ScanIterator
	ctx       *ExecutorContext
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	return &IndexScanExecutor{
		plan:      plan,
		index:     index,
		tableHeap: tableHeap,
	}
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.iter, _ = e.index.Scan(e.plan.StartKey, e.plan.Direction, ctx.txn)
	return nil
}

func (e *IndexScanExecutor) Next() bool {
	return e.iter.Next()
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	buffer := make([]byte, e.tableHeap.desc.BytesPerTuple())
	err := e.tableHeap.ReadTuple(e.ctx.txn, e.iter.Value(), buffer, e.plan.ForUpdate)
	if err != nil {
		return storage.Tuple{}
	}
	return storage.FromRawTuple(buffer, e.tableHeap.desc, e.iter.Value())
}

func (e *IndexScanExecutor) Close() error {
	err := e.iter.Close()
	if err != nil {
		return err
	}
	return nil
}

func (e *IndexScanExecutor) Error() error {
	return nil
}
