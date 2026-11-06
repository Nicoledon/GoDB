package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	// Fill me in!
	plan      *planner.IndexLookupNode
	index     indexing.Index
	tableHeap *TableHeap
	ctx       *ExecutorContext
	output    []common.RecordID
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	return &IndexLookupExecutor{
		plan:      plan,
		index:     index,
		tableHeap: tableHeap,
	}
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.output = make([]common.RecordID, 0)
	e.ctx = ctx
	e.output, _ = e.index.ScanKey(e.plan.EqualityKey, e.output, e.ctx.txn)
	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	if len(e.output) == 0 {
		return false
	}
	return true
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	item := e.output[0]
	e.output = e.output[1:]
	buffer := make([]byte, e.tableHeap.desc.BytesPerTuple())
	err := e.tableHeap.ReadTuple(e.ctx.txn, item, buffer, e.plan.ForUpdate)
	//print("output :%x", e.output)
	if err != nil {
		return storage.Tuple{}
	}
	return storage.FromRawTuple(buffer, e.tableHeap.desc, item)
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return nil
}
