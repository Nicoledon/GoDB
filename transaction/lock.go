package transaction

import (
	"fmt"
	"sync"

	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

// LockManager manages the granting, releasing, and waiting of locks on database resources.

type LockMetaData struct {
	units      []DBLockMode
	UpdateUnit DBLockMode
	Update     bool
}
type CurrentState struct {
	mode DBLockMode
	tid  common.TransactionID
}
type Mode struct {
	ModeMap []*CurrentState
	cond    *sync.Cond
}
type LockManager struct {
	// Add fields here
	WaitingQueue map[DBLockTag]*LockMetaData
	CurrentMode  map[DBLockTag]*Mode
	LockPool     sync.Pool
	TableLock    sync.Mutex
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	locker := LockManager{
		WaitingQueue: make(map[DBLockTag]*LockMetaData),
		CurrentMode:  make(map[DBLockTag]*Mode),
	}
	return &locker
}

// NOTE mode is current you wants to hold ,return true if they can co-exist
func (lm *LockManager) CheckCompatibility(mode DBLockMode, tag DBLockTag, tid common.TransactionID) bool {
	states, ok := lm.CurrentMode[tag]
	if !ok {
		panic("should  not be here")
	}
	for _, m := range states.ModeMap {
		if m.tid == tid {
			continue
		}
		switch m.mode {
		case LockModeIS:
			if mode == LockModeX {
				return false
			}
		case LockModeIX:
			if mode == LockModeS || mode == LockModeSIX || mode == LockModeX {
				return false
			}
		case LockModeS:
			if mode == LockModeIX || mode == LockModeSIX || mode == LockModeX {
				return false
			}
		case LockModeSIX:
			if mode != LockModeIS {
				return false
			}
		case LockModeX:
			return false
		}
	}
	return true
}

// NOTE : refmode is the mode you wants upgrade , if it could then return true otherwise return false
func (lm *LockManager) CheckForUpgrade(refmode DBLockMode, mode DBLockMode) bool {
	switch mode {
	case LockModeIS:
		if refmode == LockModeS || refmode == LockModeIX || refmode == LockModeSIX || refmode == LockModeX {
			return true
		}
	case LockModeS:
		if refmode == LockModeSIX || refmode == LockModeX {
			return true
		}
	case LockModeIX:
		if refmode == LockModeSIX || refmode == LockModeX {
			return true
		}
	case LockModeSIX:
		if refmode == LockModeX {
			return true
		}
	case LockModeX:
		return false
	}
	return false
}
func (lm *LockManager) InState(items []*CurrentState, tid common.TransactionID) bool {
	for _, item := range items {
		if item.tid == tid {
			return true
		}
	}
	return false
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	lm.TableLock.Lock()
	defer lm.TableLock.Unlock()
	// case1  when there is no lock on the tid
	items, ok := lm.CurrentMode[tag]
	if !ok || len(items.ModeMap) == 0 {
		lm.CurrentMode[tag] = &Mode{make([]*CurrentState, 0), sync.NewCond(&lm.TableLock)}
		lm.CurrentMode[tag].ModeMap = append(lm.CurrentMode[tag].ModeMap, &CurrentState{mode: mode, tid: tid})
		return nil
	}
	// case2 when i needs to upgrade  while checkforupgrade == false
	for _, item := range items.ModeMap {
		if item.tid == tid && !lm.CheckForUpgrade(mode, item.mode) {
			return nil
		} else if item.tid == tid && lm.CheckForUpgrade(mode, item.mode) {
			_, ok1 := lm.WaitingQueue[tag]
			if !ok1 {
				MetaData := &LockMetaData{units: make([]DBLockMode, 0), UpdateUnit: mode, Update: true}
				lm.WaitingQueue[tag] = MetaData
			}
			for !lm.CheckCompatibility(mode, tag, tid) {
				lm.WaitingQueue[tag].Update = true
				lm.WaitingQueue[tag].UpdateUnit = mode
				lm.CurrentMode[tag].cond.Wait()
			}
			for i, val := range lm.CurrentMode[tag].ModeMap {
				if val.tid == tid {
					lm.CurrentMode[tag].ModeMap[i].mode = mode
				}
			}
			return nil
		}
	}
	//case3  while  checkcompatible === false
	if !lm.CheckCompatibility(mode, tag, tid) {
		_, ok1 := lm.WaitingQueue[tag]
		if !ok1 {
			MetaData := &LockMetaData{units: make([]DBLockMode, 0), UpdateUnit: mode, Update: true}
			lm.WaitingQueue[tag] = MetaData
		}
		for !lm.CheckCompatibility(mode, tag, tid) {
			lm.WaitingQueue[tag].units = append(lm.WaitingQueue[tag].units, mode)
			lm.CurrentMode[tag].cond.Wait()
		}
		lm.CurrentMode[tag].ModeMap = append(lm.CurrentMode[tag].ModeMap, &CurrentState{mode: mode, tid: tid})
		return nil
	}
	lm.CurrentMode[tag].ModeMap = append(lm.CurrentMode[tag].ModeMap, &CurrentState{mode: mode, tid: tid})
	return nil
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	lm.TableLock.Lock()
	defer lm.TableLock.Unlock()
	items, _ := lm.CurrentMode[tag]
	for i, item := range items.ModeMap {
		if item.tid == tid {
			lm.CurrentMode[tag].ModeMap = append(lm.CurrentMode[tag].ModeMap[:i], lm.CurrentMode[tag].ModeMap[i+1:]...)
			break
		}
	}
	queue, ok := lm.WaitingQueue[tag]
	if !ok {
		return nil
	}
	if queue.Update && lm.CheckCompatibility(queue.UpdateUnit, tag, tid) {
		queue.Update = false
		lm.CurrentMode[tag].cond.Signal()
		return nil
	}
	for len(lm.WaitingQueue[tag].units) > 0 {
		if lm.CheckCompatibility(lm.WaitingQueue[tag].units[0], tag, tid) {
			lm.WaitingQueue[tag].units = lm.WaitingQueue[tag].units[1:]
			lm.CurrentMode[tag].cond.Signal()
		} else {
			break
		}
	}
	return nil
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	lm.TableLock.Lock()
	defer lm.TableLock.Unlock()
	item, ok := lm.CurrentMode[tag]
	if !ok {
		return false
	}
	if len(item.ModeMap) > 0 {
		return true
	}
	return false
}
