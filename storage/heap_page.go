package storage

import (
	"encoding/binary"
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
	var uNum uint32
	if isBigEndian {
		uNum = binary.BigEndian.Uint32(hp.Bytes[8:11])
	} else {
		uNum = binary.LittleEndian.Uint32(hp.Bytes[8:11])
	}
	return int(uNum)
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

func (frame *PageFrame) AsHeapPage() HeapPage {
	return HeapPage{
		frame,
	}
}
func FindAllocatedDeletedView(hp HeapPage) (allocatedView Bitmap, deletedview Bitmap) {
	//allocatedBytes := make([]byte, hp.AllocatedBitmapLen-4)
	//deletedBytes := make([]byte, hp.PageFrame.DeletedBitmapLen-4)
	allocatedBytes := hp.Bytes[hp.PageFrame.AllocatedBitmapStart : hp.PageFrame.AllocatedBitmapStart+hp.AllocatedBitmapLen-4]
	deletedBytes := hp.Bytes[hp.PageFrame.DeletedBitmapStart : hp.PageFrame.DeletedBitmapStart+hp.DeletedBitmapLen-4]
	var allcatedNum uint32 = 0
	var deletedNum uint32 = 0
	var isBigEndian = func() bool {
		buf := [2]byte{}
		*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
		return buf[0] == 0xCA
	}()
	if isBigEndian {
		allcatedNum = binary.BigEndian.Uint32(hp.Bytes[hp.PageFrame.AllocatedBitmapStart+hp.AllocatedBitmapLen-4 : hp.PageFrame.AllocatedBitmapStart+hp.AllocatedBitmapLen])
		deletedNum = binary.BigEndian.Uint32(hp.Bytes[hp.PageFrame.DeletedBitmapStart+hp.DeletedBitmapLen-4 : hp.PageFrame.DeletedBitmapStart+hp.DeletedBitmapLen])
	} else {
		allcatedNum = binary.LittleEndian.Uint32(hp.Bytes[hp.PageFrame.AllocatedBitmapStart+hp.AllocatedBitmapLen-4 : hp.PageFrame.AllocatedBitmapStart+hp.AllocatedBitmapLen])
		deletedNum = binary.LittleEndian.Uint32(hp.Bytes[hp.PageFrame.DeletedBitmapStart+hp.DeletedBitmapLen-4 : hp.PageFrame.DeletedBitmapStart+hp.DeletedBitmapLen])
	}
	allocatedView = AsBitmap(allocatedBytes, int(allcatedNum))
	deletedview = AsBitmap(deletedBytes, int(deletedNum))
	return allocatedView, deletedview
}
func (hp HeapPage) FindFreeSlot() int {
	allocatedView, _ := FindAllocatedDeletedView(hp)
	find := allocatedView.FindFirstZero(0)
	return find
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	allocatedView, _ := FindAllocatedDeletedView(hp)
	if allocatedView.LoadBit(int(rid.Slot)) {
		return true
	}
	return false
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
	_, deletedView := FindAllocatedDeletedView(hp)
	return deletedView.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	_, deletedView := FindAllocatedDeletedView(hp)
	deletedView.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	AccessStart := hp.DeletedBitmapStart + hp.DeletedBitmapLen + hp.RowLens*int(rid.Slot)
	data := hp.Bytes[AccessStart : AccessStart+hp.RowLens]
	return RawTuple(data)
}
