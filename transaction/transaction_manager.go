package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// activeTxnEntry tracks a running transaction and its starting point in the log.
type activeTxnEntry struct {
	txn      *TransactionContext
	startLsn storage.LSN
}

// TransactionManager is the central component managing the lifecycle of transactions.
// It coordinates with the LockManager for concurrency control and the LogManager for
// Write-Ahead Logging (WAL) and recovery.
type TransactionManager struct {
	// activeTxns maps TransactionIDs to their runtime context and metadata
	activeTxns *xsync.MapOf[common.TransactionID, activeTxnEntry]

	logManager  storage.LogManager
	bufferPool  *storage.BufferPool
	lockManager *LockManager

	nextTxnID atomic.Uint64
	// Pool to recycle transaction contexts
	txnPool sync.Pool

	// snapshotMu guards the ATT snapshot invariant.
	// Commits/Aborts hold RLock across [Append(CommitRecord/AbortRecord) … Delete(tid)].
	// GetActiveTransactionsSnapshot holds Lock for the entire Range() call.
	// This ensures no transaction can have its Commit record at LSN < checkpointLSN
	// while still appearing in the snapshot (un-deleted from activeTxns).
	snapshotMu sync.RWMutex
}

// NewTransactionManager initializes the transaction manager.
func NewTransactionManager(logManager storage.LogManager, bufferPool *storage.BufferPool, lockManager *LockManager) *TransactionManager {
	return &TransactionManager{
		logManager:  logManager,
		bufferPool:  bufferPool,
		lockManager: lockManager,
	}
}

// Begin starts a new transaction and returns the initialized context.
func (tm *TransactionManager) Begin() (*TransactionContext, error) {

	tm.nextTxnID.Add(1)
	var tid common.TransactionID = common.TransactionID(tm.nextTxnID.Load())
	ctx := NewTestTransactionContext(tm.lockManager, tid)
	tm.logManager.Append(storage.NewBeginTransactionRecord(make([]byte, storage.BeginTransactionRecordSize()), tid))
	return ctx, nil
}

// Commit completes a transaction and makes its effects durable and visible.
func (tm *TransactionManager) Commit(txn *TransactionContext) error {

	// Execute In-Memory changes (Indexes) after flushed. Think about how this should interleave with the commit logic.
	for _, task := range txn.commitActions {
		task.Target.Invoke(task.Type, task.Key, task.RID)
	}
	lsn, err := tm.logManager.Append(storage.NewCommitRecord(make([]byte, storage.CommitRecordSize()), txn.id))
	if err != nil {
		return err
	}
	err = tm.logManager.WaitUntilFlushed(lsn)
	if err != nil {
		return err
	}
	for pid, _ := range txn.heldLocks {
		err = txn.lm.Unlock(txn.id, pid)
		if err != nil {
			return err
		}
	}
	return nil
}

// Abort stops a transaction and ensures its effects are rolled back
func (tm *TransactionManager) Abort(txn *TransactionContext) error {
	// Rollback In-Memory changes (Indexes)
	// YOU SHOULD NOT NEED TO MODIFY THIS LOGIC
	for i := len(txn.abortActions) - 1; i >= 0; i-- {
		cleanupTask := txn.abortActions[i]
		cleanupTask.Target.Invoke(cleanupTask.Type, cleanupTask.Key, cleanupTask.RID)
	}
	fmt.Printf("Aborting transaction %d\n", txn.ID())
	// Add your implementation here
	lens := txn.logRecords.len()
	for i := lens - 1; i >= 0; i-- {
		events := txn.logRecords.get(i)
		if events.RecordType() == storage.LogInsert {
			page, _ := tm.bufferPool.GetPage(events.RID().PageID)
			lsn, err := tm.logManager.Append(storage.NewInsertCLR(make([]byte, storage.InsertCLRSize()), events))
			if err != nil {
				return err
			}
			page.PageLatch.Lock()
			HeapPage := page.AsHeapPage()
			HeapPage.MarkDeleted(events.RID(), true)
			page.MonotonicallyUpdateLSN(lsn)
			page.PageLatch.Unlock()
		} else if events.RecordType() == storage.LogUpdate {
			page, _ := tm.bufferPool.GetPage(events.RID().PageID)
			lsn, err := tm.logManager.Append(storage.NewUpdateCLR(make([]byte, storage.UpdateCLRSize(events)), events))
			if err != nil {
				return err
			}
			page.PageLatch.Lock()
			HeapPage := page.AsHeapPage()
			tuple := HeapPage.AccessTuple(events.RID())
			copy(tuple, events.BeforeImage())
			page.MonotonicallyUpdateLSN(lsn)
			page.PageLatch.Unlock()
		} else if events.RecordType() == storage.LogDelete {
			page, _ := tm.bufferPool.GetPage(events.RID().PageID)
			lsn, err := tm.logManager.Append(storage.NewDeleteCLR(make([]byte, storage.DeleteCLRSize()), events))
			if err != nil {
				return err
			}
			page.PageLatch.Lock()
			HeapPage := page.AsHeapPage()
			HeapPage.MarkDeleted(events.RID(), false)
			page.MonotonicallyUpdateLSN(lsn)
			page.PageLatch.Unlock()
		}
	}
	lsn, err := tm.logManager.Append(storage.NewAbortRecord(make([]byte, storage.AbortRecordSize()), txn.id))
	if err != nil {
		return err
	}
	err = tm.logManager.WaitUntilFlushed(lsn)
	if err != nil {
		return err
	}
	for pid, _ := range txn.heldLocks {
		err = txn.lm.Unlock(txn.id, pid)
		if err != nil {
			return err
		}
	}
	return nil
}

// RestartTransactionForRecovery is used during database recovery (ARIES Redo phase).
// It reconstructs a TransactionContext for a transaction that was active at the time of the crash.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) RestartTransactionForRecovery(txnId common.TransactionID) *TransactionContext {
	panic("unimplemented")
}

// ATTEntry represents a snapshot of an active transaction for the Active Transaction Table (ATT).
type ATTEntry struct {
	ID       common.TransactionID
	StartLSN storage.LSN
}

// GetActiveTransactionsSnapshot returns a snapshot of currently active transaction IDs and their start LSNs.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) GetActiveTransactionsSnapshot() []ATTEntry {
	panic("unimplemented")
}
