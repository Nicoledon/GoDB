package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
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
	buffer    []byte
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
	if e.ctx.txn != nil {
		if e.plan.ForUpdate {
			err := e.ctx.txn.AcquireLock(transaction.NewTableLockTag(e.plan.TableOid), transaction.LockModeIX)
			if err != nil {
				return err
			}
		} else {
			err := e.ctx.txn.AcquireLock(transaction.NewTableLockTag(e.plan.TableOid), transaction.LockModeIS)
			if err != nil {
				return err
			}
		}
	}
	e.iter, _ = e.index.Scan(e.plan.StartKey, e.plan.Direction, ctx.txn)
	return nil
}
func (e *IndexScanExecutor) KeyCompare() bool {
	keys := make([]byte, 0)
	for _, key := range e.index.Metadata().ProjectionList {
		tempKey := e.tableHeap.desc.GetValue(e.buffer, key)
		tempBuffer := make([]byte, tempKey.SizeInBytes())
		tempKey.WriteTo(tempBuffer)
		keys = append(keys, tempBuffer...)
	}
	key := indexing.NewKey(keys, e.index.Metadata().KeySchema)
	if !e.iter.Key().Equals(key) {
		return false
	}
	if e.plan.Direction == 0 {
		if e.plan.StartKey.Compare(key) < 1 {
			return true
		}
	} else {
		if e.plan.StartKey.Compare(key) >= 0 {
			return true
		}
	}
	return false
}
func (e *IndexScanExecutor) Next() bool {
	for e.iter.Next() {
		if e.ctx.txn != nil {
			if e.plan.ForUpdate {
				e.buffer = make([]byte, e.tableHeap.desc.BytesPerTuple())
				err := e.ctx.txn.AcquireLock(transaction.NewTupleLockTag(e.iter.Value()), transaction.LockModeX)
				if err != nil {
					panic("should not be here")
				}
				err = e.tableHeap.ReadTuple(nil, e.iter.Value(), e.buffer, e.plan.ForUpdate)
				if err != nil {
					continue
				}
				if e.KeyCompare() {
					return true
				}
				continue
			} else {
				e.buffer = make([]byte, e.tableHeap.desc.BytesPerTuple())
				err := e.ctx.txn.AcquireLock(transaction.NewTupleLockTag(e.iter.Value()), transaction.LockModeS)
				if err != nil {
					panic("should not be here")
				}
				err = e.tableHeap.ReadTuple(nil, e.iter.Value(), e.buffer, e.plan.ForUpdate)
				if err != nil {
					continue
				}
				if e.KeyCompare() {
					return true
				}
				continue
			}
		} else {
			e.buffer = make([]byte, e.tableHeap.desc.BytesPerTuple())
			err := e.tableHeap.ReadTuple(e.ctx.txn, e.iter.Value(), e.buffer, e.plan.ForUpdate)
			if err != nil {
				panic("should not be here")
			}
			return true
		}
	}
	return false
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.buffer, e.tableHeap.desc, e.iter.Value())
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
