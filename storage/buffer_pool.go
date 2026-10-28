package storage

import (
	"math"
	"sync"

	"mit.edu/dsg/godb/common"
)

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
var CurrentTime uint64 = 0

type pinCount int
type BufferPool struct {
	// add more fields here...
	numPages    int
	fileManager DBFileManager
	logManager  LogManager
	pageMap     *Lru_k_replacer
	mu          sync.Mutex
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	return &BufferPool{
		numPages:    numPages,
		fileManager: storageManager,
		logManager:  logManager,
		pageMap:     NewLRUKreplacer(numPages),
	}
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.fileManager
}

func UpdateTime(node *Node) {
	node.timestamp[1] = node.timestamp[0]
	node.timestamp[0] = CurrentTime
}
func InitNode(node *Node, frame *PageFrame, recordID common.PageID) {
	node.timestamp[0] = math.MaxUint64
	node.timestamp[1] = math.MaxUint64
	node.pins = 0
	node.frame = frame
	node.dirty = false
	node.recordID = recordID
}
func ReadPageIN(bp *BufferPool, pageID common.PageID, frame *PageFrame) {
	file, err := bp.fileManager.GetDBFile(pageID.Oid)
	if err != nil {
		panic("readpageIN panic")
	}
	err = file.ReadPage(int(pageID.PageNum), frame.Bytes[:])
	if err != nil {
		panic("readpageIN panic")
	}
}
func DeleteEvictablePage(bp *BufferPool, pageID common.PageID) int {
	var min uint64 = math.MaxUint64
	var index int = -1
	for k, v := range bp.pageMap.pageManager {
		if v.evictable && v.timestamp[1] == math.MaxUint64 {
			index = k
			break
		} else if v.evictable && v.timestamp[1] < min {
			min = v.timestamp[1]
			index = k
		}
	}
	if index == -1 {
		panic("error happend in find evictable page")
	}
	if bp.pageMap.pageManager[index].dirty {
		IDS := bp.pageMap.pageManager[index].recordID
		file, _ := bp.fileManager.GetDBFile(IDS.Oid)
		err := file.WritePage(int(IDS.PageNum), bp.pageMap.pageManager[index].frame.Bytes[:])
		if err != nil {
			panic("error occur")
		}
	}
	return index
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	bp.mu.Lock()
	page, ok := bp.pageMap.pageManager[int(pageID.PageNum)]
	// case 1 当成功找到了这个PAGE 之后
	if ok {
		UpdateTime(page)
		CurrentTime += 1
		page.pins += 1
		page.evictable = false
		page.frame.pageID = int(pageID.PageNum)
		bp.mu.Unlock()
		return page.frame, nil
	} else if len(bp.pageMap.pageManager) < bp.numPages {
		// case 2 当pageMap 没有满的时候 ，并且没找到在这个pageMap 里面
		frame := PageFrame{}
		node := Node{}
		ReadPageIN(bp, pageID, &frame)
		InitNode(&node, &frame, pageID)
		node.pins += 1
		node.evictable = false
		bp.pageMap.pageManager[int(pageID.PageNum)] = &node
		CurrentTime += 1
		frame.pageID = int(pageID.PageNum)
		bp.mu.Unlock()
		return &frame, nil
	}
	index := DeleteEvictablePage(bp, pageID)
	frame := bp.pageMap.pageManager[index].frame
	frame.pageID = int(pageID.PageNum)
	node := bp.pageMap.pageManager[index]
	ReadPageIN(bp, pageID, frame)
	InitNode(node, frame, pageID)
	node.pins += 1
	node.evictable = false
	bp.pageMap.pageManager[int(pageID.PageNum)] = node
	delete(bp.pageMap.pageManager, index)
	CurrentTime += 1
	bp.mu.Unlock()
	return frame, nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	bp.mu.Lock()
	bp.pageMap.pageManager[frame.pageID].pins -= 1
	bp.pageMap.pageManager[frame.pageID].dirty = setDirty
	if bp.pageMap.pageManager[frame.pageID].pins == 0 {
		bp.pageMap.pageManager[frame.pageID].evictable = true
	}
	bp.mu.Unlock()
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	for _, v := range bp.pageMap.pageManager {
		if v.dirty {
			file, _ := bp.fileManager.GetDBFile(v.recordID.Oid)
			err := file.WritePage(int(v.recordID.PageNum), v.frame.Bytes[:])
			if err != nil {
				return err
			}
		}
	}
	bp.mu.Unlock()
	return nil
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is called during checkpoint to snapshot the current DPT into the log.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}
