package execution

import (
	"errors"
	"sync"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager
	PageNum     int32
	cap         int32
	dataLock    sync.RWMutex
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	types := make([]common.Type, 0)
	for _, t := range table.Columns {
		types = append(types, t.Type)
	}
	desc := storage.NewRawTupleDesc(types)
	file, _ := bufferPool.FileManager.GetDBFile(table.Oid)
	_, _ = file.AllocatePage(bufferPool.NumPages)
	Tableheap := TableHeap{
		oid:         table.Oid,
		bufferPool:  bufferPool,
		logManager:  logManager,
		lockManager: lockManager,
		desc:        desc,
		PageNum:     0,
		cap:         int32(bufferPool.NumPages),
	}
	return &Tableheap, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	for {
		tableHeap.dataLock.Lock()
		if tableHeap.PageNum >= tableHeap.cap {
			tableHeap.cap += 1
			file, _ := tableHeap.bufferPool.FileManager.GetDBFile(tableHeap.oid)
			_, _ = file.AllocatePage(1)
		}
		page, _ := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: tableHeap.PageNum})
		page.PageLatch.Lock()
		_, ok := tableHeap.bufferPool.InitMap[tableHeap.PageNum]
		if !ok {
			storage.InitializeHeapPage(tableHeap.desc, page)
			tableHeap.bufferPool.InitMap[tableHeap.PageNum] = struct{}{}
		}
		HeapPage := page.AsHeapPage()
		pos := HeapPage.FindFreeSlot()
		if pos == -1 {
			tableHeap.PageNum += 1
			page.PageLatch.Unlock()
			tableHeap.dataLock.Unlock()
			tableHeap.bufferPool.UnpinPage(page, false)
			continue
		} else {
			id := common.RecordID{PageID: common.PageID{Oid: tableHeap.oid, PageNum: tableHeap.PageNum}, Slot: int32(pos)}
			tableHeap.dataLock.Unlock()
			PageTuple := HeapPage.AccessTuple(id)
			HeapPage.MarkAllocated(id, true)
			copy(PageTuple, row)
			page.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(page, true)
			return id, nil
		}
	}
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	page, _ := tableHeap.bufferPool.GetPage(rid.PageID)
	page.PageLatch.Lock()
	HeapPage := page.AsHeapPage()
	if HeapPage.IsDeleted(rid) {
		page.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(page, false)
		return ErrTupleDeleted
	}
	HeapPage.MarkDeleted(rid, true)
	page.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	page, _ := tableHeap.bufferPool.GetPage(rid.PageID)
	if forUpdate {
		page.PageLatch.Lock()
	} else {
		page.PageLatch.RLock()
	}
	HeapPage := page.AsHeapPage()
	if HeapPage.IsDeleted(rid) {
		if forUpdate {
			page.PageLatch.Unlock()
		} else {
			page.PageLatch.RUnlock()
		}
		tableHeap.bufferPool.UnpinPage(page, false)
		return ErrTupleDeleted
	}
	tuple := HeapPage.AccessTuple(rid)
	copy(buffer, tuple)
	if forUpdate {
		page.PageLatch.Unlock()
	} else {
		page.PageLatch.RUnlock()
	}
	tableHeap.bufferPool.UnpinPage(page, forUpdate)
	return nil
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	page, _ := tableHeap.bufferPool.GetPage(rid.PageID)
	page.PageLatch.Lock()
	HeapPage := page.AsHeapPage()
	if HeapPage.IsDeleted(rid) {
		page.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(page, false)
		return ErrTupleDeleted
	}
	tuple := HeapPage.AccessTuple(rid)
	copy(tuple, updatedTuple)
	page.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	return TableHeapIterator{buffer: buffer, countPage: 0, countPos: 0, tableHeap: tableHeap, oid: tableHeap.oid}, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	// Fill me in!
	buffer    []byte
	countPage int
	countPos  int
	tableHeap *TableHeap
	oid       common.ObjectID
	current   common.RecordID
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	panic("unimplemented")
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	it.tableHeap.dataLock.Lock()
	for {
		_, ok := it.tableHeap.bufferPool.InitMap[int32(it.countPage)]
		if !ok {
			it.tableHeap.dataLock.Unlock()
			return false
		}
		page, _ := it.tableHeap.bufferPool.GetPage(common.PageID{Oid: it.oid, PageNum: int32(it.countPage)})
		page.PageLatch.RLock()
		find := page.AsHeapPage().FindAllocatedSlot(it.countPos)
		if find == -1 {
			it.countPos = 0
			it.countPage += 1
			page.PageLatch.RUnlock()
			it.tableHeap.bufferPool.UnpinPage(page, false)
		} else {
			it.countPos = find
			recordId := common.RecordID{
				common.PageID{
					it.oid,
					int32(it.countPage),
				},
				int32(it.countPos),
			}
			copy(it.buffer, page.AsHeapPage().AccessTuple(recordId))
			it.current = recordId
			numbits := page.AsHeapPage().NumSlots()
			if it.countPos >= numbits-1 {
				it.countPos = 0
				it.countPage += 1
			} else {
				it.countPos += 1
			}
			page.PageLatch.RUnlock()
			it.tableHeap.dataLock.Unlock()
			it.tableHeap.bufferPool.UnpinPage(page, false)
			return true
		}
	}
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	it.tableHeap.dataLock.Lock()
	buffer := it.buffer
	it.tableHeap.dataLock.Unlock()
	return buffer
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	it.tableHeap.dataLock.Lock()
	recordId := it.current
	it.tableHeap.dataLock.Unlock()
	return recordId
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	panic("unimplemented")
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	return nil
}
