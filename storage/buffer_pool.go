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
	cond        *sync.Cond
	cond_page   *sync.Cond
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
		pageMap:     NewLRUKreplacer(numPages),
	}
	pool.cond = sync.NewCond(&pool.mu)
	pool.cond_page = sync.NewCond(&pool.mu)
	return &pool
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
	bp.mu.Lock()
	frame.Loading = false
	bp.cond.Broadcast()
	bp.mu.Unlock()
}

func DeleteEvictablePage(bp *BufferPool) int {
	//var Min uint64 = math.MaxUint64
	for {
		for len(bp.pageMap.underKqueue) > 0 { // 等价于 len(q) != 0
			node := bp.pageMap.underKqueue[0]
			bp.pageMap.underKqueue = bp.pageMap.underKqueue[1:] // 头部 pop，O(1)
			_, ok := bp.pageMap.pageManager[int(node.recordID.PageNum)]
			if ok && node.timestamp[1] == math.MaxUint64 && node.evictable {
				return int(node.recordID.PageNum)
			}
			// 处理 v
		}
		for len(bp.pageMap.aboveKqueue) > 0 {
			for len(bp.pageMap.aboveKqueue) > 0 { // 等价于 len(q) != 0
				node := bp.pageMap.aboveKqueue[0]
				bp.pageMap.aboveKqueue = bp.pageMap.aboveKqueue[1:] // 头部 pop，O(1)
				_, ok := bp.pageMap.pageManager[int(node.recordID.PageNum)]
				if ok && node.evictable {
					return int(node.recordID.PageNum)
				}
			}
		}
		bp.cond_page.Wait()

	}
	panic("we fucked")
}

func WritePageOutAndReadPageIn(bp *BufferPool, NewPageID common.PageID, OldPageID common.PageID, frame *PageFrame) {
	NewFile, _ := bp.fileManager.GetDBFile(NewPageID.Oid)
	OldFile, _ := bp.fileManager.GetDBFile(OldPageID.Oid)
	OldFile.WritePage(int(OldPageID.PageNum), frame.Bytes[:])
	NewFile.ReadPage(int(NewPageID.PageNum), frame.Bytes[:])
	bp.mu.Lock()
	frame.Loading = false
	bp.cond.Broadcast()
	bp.mu.Unlock()
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
		bp.pageMap.aboveKqueue = append(bp.pageMap.aboveKqueue, page)
		for page.frame.Loading {
			bp.cond.Wait()
		}
		bp.mu.Unlock()
		return page.frame, nil
	} else if len(bp.pageMap.pageManager) < bp.numPages {
		// case 2 当pageMap 没有满的时候 ，并且没找到在这个pageMap 里面
		frame := PageFrame{}
		node := Node{}
		InitNode(&node, &frame, pageID)
		node.pins += 1
		node.evictable = false
		bp.pageMap.pageManager[int(pageID.PageNum)] = &node
		UpdateTime(&node)
		CurrentTime += 1
		frame.pageID = int(pageID.PageNum)
		frame.Loading = true
		bp.pageMap.underKqueue = append(bp.pageMap.underKqueue, &node)
		bp.mu.Unlock()
		ReadPageIN(bp, pageID, &frame)
		return &frame, nil
	}
	index := DeleteEvictablePage(bp)
	dirty := bp.pageMap.pageManager[index].dirty
	OldPage := bp.pageMap.pageManager[index].recordID
	frame := bp.pageMap.pageManager[index].frame
	frame.pageID = int(pageID.PageNum)
	node := bp.pageMap.pageManager[index]
	InitNode(node, frame, pageID)
	node.pins += 1
	node.evictable = false
	node.frame.Loading = true
	bp.pageMap.pageManager[int(pageID.PageNum)] = node
	delete(bp.pageMap.pageManager, index)
	UpdateTime(node)
	CurrentTime += 1
	bp.pageMap.underKqueue = append(bp.pageMap.underKqueue, node)
	bp.mu.Unlock()
	if dirty {
		WritePageOutAndReadPageIn(bp, pageID, OldPage, frame)
	} else {
		ReadPageIN(bp, pageID, frame)
	}
	return frame, nil
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
	}
	bp.mu.Unlock()
	bp.cond_page.Signal()
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	for _, v := range bp.pageMap.pageManager {
		if v.dirty {
			file, _ := bp.fileManager.GetDBFile(v.recordID.Oid)
			//v.frame.PageLock.Lock()
			err := file.WritePage(int(v.recordID.PageNum), v.frame.Bytes[:])
			if err != nil {
				return err
			}
			v.dirty = false
			//v.frame.PageLock.Unlock()
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
