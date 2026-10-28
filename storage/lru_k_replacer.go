package storage

type Node struct {
	timestamp [2]uint64
	evictable bool
	pins      pinCount
	frame     *PageFrame
}
type Lru_k_replacer struct {
	// add more fields here...
	numPage     int
	pageManager map[int]*Node
}

//	func (replacer *lru_k_replacer) StorageManager() DBFileManager {
//		return bp.fileManager
//	}
func NewLRUKreplacer(numPages int) *Lru_k_replacer {
	return &Lru_k_replacer{
		numPage:     numPages,
		pageManager: make(map[int]*Node),
	}
}
