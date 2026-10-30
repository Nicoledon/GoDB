package storage

import (
	"mit.edu/dsg/godb/common"
)

type Node struct {
	pins      pinCount
	frame     *PageFrame
	pos       int
	dirty     bool
	ref       bool
	evictable bool
	valid     bool
}

type clock_replacer struct {
	// add more fields here...
	numPage     int
	pageManager map[common.PageID]*Node
	clock_page  []*Node
	clock_hand  int
}

func NewClockreplacer(numPages int) *clock_replacer {
	clock_page := make([]*Node, numPages)
	for i := 0; i < numPages; i++ {
		node := Node{}
		node.pos = i
		node.ref = true
		clock_page[i] = &node
	}
	return &clock_replacer{
		numPage:     numPages,
		pageManager: make(map[common.PageID]*Node),
		clock_page:  clock_page,
		clock_hand:  0,
	}
}
