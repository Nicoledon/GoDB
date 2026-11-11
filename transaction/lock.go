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

type LockUint struct {
	cond *sync.Cond
	mode DBLockMode
	Lock sync.Mutex
}
type LockMetaData struct {
	units      []*LockUint
	UpdateUnit *LockUint
	Update     bool
}
type CurrentState struct {
	mode DBLockMode
	tid  common.TransactionID
}
type LockManager struct {
	// Add fields here
	WaitingQueue map[DBLockTag]*LockMetaData
	CurrentMode  map[DBLockTag][]*CurrentState
	LockPool     sync.Pool
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		WaitingQueue: make(map[DBLockTag]*LockMetaData, 0),
		CurrentMode:  make(map[DBLockTag][]*CurrentState, 0),
		LockPool: sync.Pool{
			New: func() any {
				l := &LockUint{}
				l.cond.L = &l.Lock
				return l
			},
		},
	}
}
func (lm *LockManager) CheckCompatibility(mode DBLockMode, tag DBLockTag) bool {
	states, ok := lm.CurrentMode[tag]
	if !ok {
		panic("should  not be here")
	}
	for _, m := range states {
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
func (lm *LockManager) CheckForUpgrade(tid common.TransactionID, mode DBLockMode, tag DBLockTag) bool {
	items, _ := lm.CurrentMode[tag]
	for _, item := range items {
		if item.tid == tid {
			switch item.mode {
			case LockModeIS:
				if mode == LockModeS || mode == LockModeIX || mode == LockModeSIX || mode == LockModeX {
					return false
				}
			case LockModeS:
				if mode == LockModeSIX || mode == LockModeX {
					return false
				}

			case LockModeIX:
				if mode == LockModeSIX || mode == LockModeX {
					return false
				}
			case LockModeSIX:
				if mode == LockModeX {
					return false
				}
			case LockModeX:
				return true
			}
		}
	}
	return false
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	states, ok1 := lm.CurrentMode[tag]
	if !ok1 {
		current := make([]*CurrentState, 0)
		current = append(current, &CurrentState{mode: mode, tid: tid})
		lm.CurrentMode[tag] = current
		return nil
	}
	item, ok := lm.WaitingQueue[tag]
	for _, state := range states {
		if state.tid == tid {
			if lm.CheckForUpgrade(tid, mode, tag) { // TODO fix the issue checkfor upgrade should do noting
				if !ok {
					MetaData := LockMetaData{
						units:      make([]*LockUint, 0),
						UpdateUnit: nil,
					}
					theme := &LockUint{mode: mode}
					theme.cond = sync.NewCond(&theme.Lock)
					theme.Lock.Lock()
					MetaData.UpdateUnit = theme
					MetaData.Update = true
					lm.WaitingQueue[tag] = &MetaData
					theme.cond.Wait()
				} else {
					unit := &LockUint{mode: mode}
					unit.cond = sync.NewCond(&unit.Lock)
					unit.Lock.Lock()
					item.UpdateUnit = unit
					item.Update = true
					unit.cond.Wait()
				}
				states = append(states, &CurrentState{
					mode: mode,
					tid:  tid,
				})
				return nil
			}
			states = append(states, &CurrentState{
				mode: mode,
				tid:  tid,
			})
			return nil
		}
	}

	if !lm.CheckCompatibility(mode, tag) {
		if !ok {
			MetaData := LockMetaData{
				units:      make([]*LockUint, 0),
				UpdateUnit: nil,
			}
			theme := &LockUint{mode: mode}
			theme.cond = sync.NewCond(&theme.Lock)
			theme.Lock.Lock()
			MetaData.units = append(MetaData.units, theme)
			lm.WaitingQueue[tag] = &MetaData
			theme.cond.Wait()
		} else {
			unit := &LockUint{mode: mode}
			unit.cond = sync.NewCond(&unit.Lock)
			unit.Lock.Lock()
			item.units = append(item.units, unit)
			unit.cond.Wait()
		}
		states = append(states, &CurrentState{
			mode: mode,
			tid:  tid,
		})
		return nil
	}
	states = append(states, &CurrentState{
		mode: mode,
		tid:  tid,
	})
	return nil
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	states, _ := lm.CurrentMode[tag]
	for i, state := range states {
		if state.tid == tid {
			states = append(states[:i], states[i+1:]...)
			break
		}
	}
	waiter, _ := lm.WaitingQueue[tag]
	if waiter.Update {
		waiter.Update = false
		waiter.UpdateUnit.cond.Signal()
	} else {
		for len(waiter.units) > 0 {
			if !lm.CheckCompatibility(waiter.units[0].mode, tag) {
				waiter.units[0].cond.Signal()
				waiter.units = waiter.units[1:]
			}
		}
	}
	return nil
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	states, ok := lm.CurrentMode[tag]
	if !ok {
		return false
	}
	if len(states) > 0 {
		return true
	}
	return false
}
