package storage

import "mit.edu/dsg/godb/common"

type Node struct {
	timestamp [2]uint64
	evictable bool
	pins      pinCount
	frame     *PageFrame
	dirty     bool
	recordID  common.PageID
}
type Lru_k_replacer struct {
	// add more fields here...
	numPage     int
	pageManager map[int]*Node
}

func NewLRUKreplacer(numPages int) *Lru_k_replacer {
	return &Lru_k_replacer{
		numPage:     numPages,
		pageManager: make(map[int]*Node),
	}
}
