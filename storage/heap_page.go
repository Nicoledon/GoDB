package storage

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame
}

func (hp HeapPage) NumUsed() int {
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	var uNum uint16
	if isBigEndian {
		uNum = binary.BigEndian.Uint16(hp.Bytes[12:14])
	} else {
		uNum = binary.LittleEndian.Uint16(hp.Bytes[12:14])
	}
	return int(uNum)
}

func (hp HeapPage) setNumUsed(numUsed int) {
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	if isBigEndian {
		binary.BigEndian.PutUint16(hp.Bytes[12:14], uint16(numUsed))
	} else {
		binary.LittleEndian.PutUint16(hp.Bytes[12:14], uint16(numUsed))
	}
}

func (hp HeapPage) NumSlots() int {
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	var uNum uint16
	if isBigEndian {
		uNum = binary.BigEndian.Uint16(hp.Bytes[10:12])
	} else {
		uNum = binary.LittleEndian.Uint16(hp.Bytes[10:12])
	}
	return int(uNum)
}

func (hp HeapPage) RowSize() int {
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	var uNum uint16
	if isBigEndian {
		uNum = binary.BigEndian.Uint16(hp.Bytes[8:10])
	} else {
		uNum = binary.LittleEndian.Uint16(hp.Bytes[8:10])
	}
	return int(uNum)
}
func (hp HeapPage) bitmapLayout() (allocStart, allocLen, delStart, delLen int) {
	numSlots := hp.NumSlots()
	if numSlots == 0 {
		return 0, 0, 0, 0
	}
	entry := (numSlots + 63) / 64
	allocStart = 16
	allocLen = entry*8 + 4
	allocedend := 16 + entry*8
	padding := 20 + entry*8
	padding = common.Align8(padding) - padding
	delStart = allocedend + 4 + padding
	delLen = entry*8 + 4
	return allocStart, allocLen, delStart, delLen
}
func setRowSlots(RowSize uint16, frame *PageFrame) {
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	if isBigEndian {
		binary.BigEndian.PutUint16(frame.Bytes[8:10], RowSize)
	} else {
		binary.LittleEndian.PutUint16(frame.Bytes[8:10], RowSize)
	}
}
func setNumSlots(RowSize uint16, frame *PageFrame) {
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	if isBigEndian {
		binary.BigEndian.PutUint16(frame.Bytes[10:12], RowSize)
	} else {
		binary.LittleEndian.PutUint16(frame.Bytes[10:12], RowSize)
	}
}
func CalculateMaxTuples(RowSize int) int {
	var N int = int(float64(common.PageSize) / (1.0/4.0 + float64(RowSize)))
	for {
		padding := 20 + (N+63)/8
		padding = common.Align8(padding) - padding
		if (N+63)/4+N*RowSize+padding <= common.PageSize-24 &&
			(N+64)/4+(N+1)*RowSize+padding > common.PageSize-24 {
			return N
		} else if (N+63)/4+N*RowSize+padding > common.PageSize-24 {
			N -= 1
		} else {
			N += 1
		}
	}
}
func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	setRowSlots(uint16(desc.bytesPerRow), frame)
	RowSize := CalculateMaxTuples(desc.bytesPerRow)
	if RowSize > 0 {
		frame.success = true
	} else {
		frame.success = false
	}
	var entry int = (RowSize + 63) / 64
	setNumSlots(uint16(RowSize), frame)
	zeros := make([]byte, entry*8)
	allocedend := 16 + entry*8
	padding := 20 + entry*8
	padding = common.Align8(padding) - padding
	deletestart := allocedend + 4 + padding
	deletesend := deletestart + entry*8
	frame.AllocatedBitmapStart = 16
	frame.AllocatedBitmapLen = entry*8 + 4
	frame.DeletedBitmapStart = deletestart
	frame.DeletedBitmapLen = entry*8 + 4
	copy(frame.Bytes[16:allocedend], zeros)
	frame.RowLens = desc.bytesPerRow
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	if isBigEndian {
		binary.BigEndian.PutUint32(frame.Bytes[allocedend:allocedend+4], uint32(RowSize))
		binary.BigEndian.PutUint32(frame.Bytes[deletesend:deletesend+4], uint32(RowSize))
	} else {
		binary.LittleEndian.PutUint32(frame.Bytes[allocedend:allocedend+4], uint32(RowSize))
		binary.LittleEndian.PutUint32(frame.Bytes[deletesend:deletesend+4], uint32(RowSize))
	}
	copy(frame.Bytes[deletestart:deletesend], zeros)
}

func InitializeHeapPageMetadata(desc *RawTupleDesc, frame *PageFrame) {
	RowSize := CalculateMaxTuples(desc.bytesPerRow)
	var entry int = (RowSize + 63) / 64
	allocedend := 16 + entry*8
	padding := 20 + entry*8
	padding = common.Align8(padding) - padding
	deletestart := allocedend + 4 + padding
	frame.AllocatedBitmapStart = 16
	frame.AllocatedBitmapLen = entry*8 + 4
	frame.DeletedBitmapStart = deletestart
	frame.DeletedBitmapLen = entry*8 + 4
	frame.RowLens = desc.bytesPerRow
}
func (frame *PageFrame) AsHeapPage() HeapPage {
	return HeapPage{
		frame,
	}
}
func FindAllocatedDeletedView(hp HeapPage) (allocatedView Bitmap, deletedview Bitmap) {
	allocStart, allocLen, delStart, delLen := hp.bitmapLayout()
	if allocLen == 0 {
		return Bitmap{}, Bitmap{}
	}
	allocatedBytes := hp.Bytes[allocStart : allocStart+allocLen-4]
	deletedBytes := hp.Bytes[delStart : delStart+delLen-4]
	var allcatedNum uint32 = 0
	var deletedNum uint32 = 0
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	if isBigEndian {
		allcatedNum = binary.BigEndian.Uint32(hp.Bytes[allocStart+allocLen-4 : allocStart+allocLen])
		deletedNum = binary.BigEndian.Uint32(hp.Bytes[delStart+delLen-4 : delStart+delLen])
	} else {
		allcatedNum = binary.LittleEndian.Uint32(hp.Bytes[allocStart+allocLen-4 : allocStart+allocLen])
		deletedNum = binary.LittleEndian.Uint32(hp.Bytes[delStart+delLen-4 : delStart+delLen])
	}
	allocatedView = AsBitmap(allocatedBytes, int(allcatedNum))
	deletedview = AsBitmap(deletedBytes, int(deletedNum))
	return allocatedView, deletedview
}

func (hp HeapPage) FindFreeSlot() int {
	if hp.NumSlots() == 0 {
		return -1
	}
	allocatedView, _ := FindAllocatedDeletedView(hp)
	find := allocatedView.FindFirstZero(0)
	if allocatedView.numBits <= 0 {
		fmt.Printf("Error: PageID %d has invalid numBits: %d\n", hp.pageID, allocatedView.numBits)
		panic("erro")
	}
	return find
}
func (hp HeapPage) FindAllocatedSlot(slot int) int {
	if hp.NumSlots() == 0 {
		return -1
	}
	allocatedView, deletedView := FindAllocatedDeletedView(hp)
	find := FindFirstAllocate(&allocatedView, &deletedView, slot)
	return find
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	if hp.NumSlots() == 0 || int(rid.Slot) >= hp.NumSlots() {
		return false
	}
	allocatedView, _ := FindAllocatedDeletedView(hp)
	return allocatedView.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	allocatedView, deletedView := FindAllocatedDeletedView(hp)
	allocatedView.SetBit(int(rid.Slot), allocated)
	if allocated {
		hp.setNumUsed(hp.NumUsed() + 1)
	} else {
		deletedView.SetBit(int(rid.Slot), allocated)
		hp.setNumUsed(hp.NumUsed() - 1)
	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	if hp.NumSlots() == 0 || int(rid.Slot) >= hp.NumSlots() {
		return false
	}
	_, deletedView := FindAllocatedDeletedView(hp)
	return deletedView.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	_, deletedView := FindAllocatedDeletedView(hp)
	deletedView.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	_, _, delStart, delLen := hp.bitmapLayout()
	rowLen := hp.RowSize()
	AccessStart := delStart + delLen + rowLen*int(rid.Slot)
	data := hp.Bytes[AccessStart : AccessStart+rowLen]
	return RawTuple(data)
}
