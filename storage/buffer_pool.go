package storage

import (
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
type pinCount int

type BufferPool struct {
	// add more fields here...
	numPages    int
	fileManager DBFileManager
	logManager  LogManager
	pageMap     *clock_replacer
	mu          sync.Mutex
	cond        *sync.Cond
	//cond_page   *sync.Cond
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	pool := BufferPool{
		numPages:    numPages,
		fileManager: storageManager,
		logManager:  logManager,
		pageMap:     NewClockreplacer(numPages),
	}
	pool.cond = sync.NewCond(&pool.mu)
	return &pool
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.fileManager
}

//func UpdateTime(node *Node) {
//	node.timestamp[1] = node.timestamp[0]
//	node.timestamp[0] = CurrentTime
//}

func InitNode(node *Node, frame *PageFrame, recordID common.PageID) {
	//node.timestamp[0] = math.MaxUint64
	//node.timestamp[1] = math.MaxUint64
	node.pins = 0
	node.frame = frame
	node.dirty = false
	node.pins += 1
	node.evictable = false
	node.valid = true
	frame.pageID = recordID
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

func DeleteEvictablePage(bp *BufferPool) (common.PageID, *Node, bool) {
	for {
		for i := 0; i < bp.numPages*2; i++ {
			hand := bp.pageMap.clock_hand
			if !bp.pageMap.clock_page[hand].valid {
				bp.pageMap.clock_hand = (hand + 1) % bp.numPages
				return common.PageID{}, bp.pageMap.clock_page[hand], true
			} else if bp.pageMap.clock_page[hand].evictable && !bp.pageMap.clock_page[hand].ref {
				bp.pageMap.clock_hand = (hand + 1) % bp.numPages
				return bp.pageMap.clock_page[hand].frame.pageID, bp.pageMap.clock_page[hand], true
			} else if bp.pageMap.clock_page[hand].evictable && bp.pageMap.clock_page[hand].ref {
				bp.pageMap.clock_page[hand].ref = false
			}
			hand = (hand + 1) % bp.numPages
			bp.pageMap.clock_hand = hand
		}
		bp.cond.Wait()
		return common.PageID{}, nil, false
	}
}

func WritePageOutAndReadPageIn(bp *BufferPool, NewPageID common.PageID, OldPageID common.PageID, frame *PageFrame) {
	NewFile, _ := bp.fileManager.GetDBFile(NewPageID.Oid)
	OldFile, _ := bp.fileManager.GetDBFile(OldPageID.Oid)
	OldFile.WritePage(int(OldPageID.PageNum), frame.Bytes[:])
	NewFile.ReadPage(int(NewPageID.PageNum), frame.Bytes[:])
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	bp.mu.Lock()
	for {
		page, ok := bp.pageMap.pageManager[pageID]
		// case 1 当成功找到了这个PAGE 之后
		if ok {
			page.pins += 1
			page.evictable = false
			page.frame.pageID = pageID
			page.frame.PageLock.Lock()
			frame := page.frame
			bp.mu.Unlock()
			page.frame.PageLock.Unlock()
			return frame, nil
		} else if len(bp.pageMap.pageManager) < bp.numPages {
			_, node, _ := DeleteEvictablePage(bp)
			frame := PageFrame{}
			node.frame = &frame
			InitNode(node, &frame, pageID)
			bp.pageMap.pageManager[pageID] = node
			frame.PageLock.Lock()
			bp.mu.Unlock()
			ReadPageIN(bp, pageID, &frame)
			frame.PageLock.Unlock()
			return &frame, nil
		}
		index, _, ok := DeleteEvictablePage(bp)
		if !ok {
			continue
		}
		node := bp.pageMap.pageManager[index]
		frame := bp.pageMap.pageManager[index].frame
		dirty := bp.pageMap.pageManager[index].dirty
		InitNode(node, frame, pageID)
		bp.pageMap.pageManager[pageID] = node
		delete(bp.pageMap.pageManager, index)
		frame.PageLock.Lock()
		bp.mu.Unlock()
		if dirty {
			WritePageOutAndReadPageIn(bp, pageID, index, frame)
		} else {
			ReadPageIN(bp, pageID, frame)
		}
		frame.PageLock.Unlock()
		return frame, nil
	}
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	bp.mu.Lock()
	bp.pageMap.pageManager[frame.pageID].pins -= 1
	if !bp.pageMap.pageManager[frame.pageID].dirty {
		bp.pageMap.pageManager[frame.pageID].dirty = setDirty
	}
	if bp.pageMap.pageManager[frame.pageID].pins == 0 {
		bp.pageMap.pageManager[frame.pageID].evictable = true
		bp.cond.Signal()
	}
	bp.mu.Unlock()
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	for _, v := range bp.pageMap.pageManager {
		if v.dirty {
			file, _ := bp.fileManager.GetDBFile(v.frame.pageID.Oid)
			err := file.WritePage(int(v.frame.pageID.PageNum), v.frame.Bytes[:])
			if err != nil {
				return err
			}
			v.dirty = false
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
