package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
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
	tuple     common.RecordID
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
	if e.ctx.txn != nil {
		if e.plan.ForUpdate {
			err := e.ctx.txn.AcquireLock(transaction.NewTableLockTag(e.plan.TableOid), transaction.LockModeIX)
			if err != nil {
				return err
			}
		}
		err := e.ctx.txn.AcquireLock(transaction.NewTableLockTag(e.plan.TableOid), transaction.LockModeIS)
		if err != nil {
			return err
		}
	}
	e.output, _ = e.index.ScanKey(e.plan.EqualityKey, e.output, e.ctx.txn)
	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	for {
		if len(e.output) == 0 {
			return false
		}
		e.tuple = e.output[0]
		e.output = e.output[1:]
		if e.ctx.txn != nil {
			if e.plan.ForUpdate {
				err := e.ctx.txn.AcquireLock(transaction.NewTupleLockTag(e.tuple), transaction.LockModeX)
				if err != nil {
					continue
				}
				buffer := make([]byte, e.tableHeap.desc.BytesPerTuple())
				err = e.tableHeap.ReadTuple(nil, e.tuple, buffer, false)
				if err != nil {
					continue
				}
				tupleKey := indexing.NewKey(buffer, e.tableHeap.desc)
				if !e.plan.EqualityKey.Equals(tupleKey) {
					continue
				}
			} else {
				err := e.ctx.txn.AcquireLock(transaction.NewTupleLockTag(e.tuple), transaction.LockModeS)
				if err != nil {
					continue
				}
				buffer := make([]byte, e.tableHeap.desc.BytesPerTuple())
				err = e.tableHeap.ReadTuple(nil, e.tuple, buffer, false)
				if err != nil {
					continue
				}
				tupleKey := indexing.NewKey(buffer, e.tableHeap.desc)
				if !e.plan.EqualityKey.Equals(tupleKey) {
					continue
				}
			}
		}
		return true
	}
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	item := e.tuple
	buffer := make([]byte, e.tableHeap.desc.BytesPerTuple())
	err := e.tableHeap.ReadTuple(e.ctx.txn, item, buffer, e.plan.ForUpdate)
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
