package storage

import (
	"mit.edu/dsg/godb/common"
)

//type Node struct {
//	pins      pinCount
//	frame     *PageFrame
//	pos       int
//	dirty     bool
//	ref       bool
//	evictable bool
//	valid     bool
//}

type clock_replacer struct {
	// add more fields here...
	numPage     int
	pageManager map[common.PageID]*PageFrame
	clock_page  []*PageFrame
	clock_hand  int
}

func NewClockreplacer(numPages int) *clock_replacer {
	clock_page := make([]*PageFrame, numPages)
	for i := 0; i < numPages; i++ {
		node := PageFrame{}
		node.pos = i
		node.ref = false
		clock_page[i] = &node
	}
	return &clock_replacer{
		numPage:     numPages,
		pageManager: make(map[common.PageID]*PageFrame),
		clock_page:  clock_page,
		clock_hand:  0,
	}
}
