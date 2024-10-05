package godb

import (
	"bytes"
	"encoding/binary"
	"sync"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	sync.Mutex
	pageNo        int
	file          *HeapFile
	desc          *TupleDesc
	bytesPerTuple int
	data          []*Tuple
	usedSlots     int
	totalSlots    int
	extraBytes    int
	dirty         bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	newPage := heapPage{}

	newPage.pageNo = pageNo
	newPage.file = f
	newPage.desc = desc
	newPage.usedSlots = 0
	newPage.dirty = false

	newPage.bytesPerTuple = 0

	for _, field := range desc.Fields {
		switch field.Ftype {
		case StringType:
			newPage.bytesPerTuple += int(unsafe.Sizeof(byte('a'))) * StringLength
		case IntType:
			newPage.bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
	}

	const RemPageSize = PageSize - 8

	newPage.totalSlots = RemPageSize / newPage.bytesPerTuple
	newPage.extraBytes = RemPageSize - newPage.totalSlots*newPage.bytesPerTuple
	newPage.data = make([]*Tuple, newPage.totalSlots)

	return &newPage, nil
}

func (h *heapPage) getNumUsedSlots() int {
	return h.usedSlots
}

func (h *heapPage) getNumSlots() int {
	return h.totalSlots
}

func (h *heapPage) nextFreeSlot() (int, error) {
	for index, value := range h.data {
		if value == nil {
			return index, nil
		}
	}

	return 0, GoDBError{PageFullError, "No free slot"}
}

type RID struct {
	pageNo int
	slotNo int
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.getNumUsedSlots() >= h.getNumSlots() {
		return 0, GoDBError{PageFullError, "No free slot"}
	}

	slot, slotFindError := h.nextFreeSlot()

	if slotFindError != nil {
		return 0, slotFindError
	}

	h.data[slot] = t
	rid := RID{h.pageNo, slot}
	t.Rid = rid
	h.usedSlots++

	return rid, nil
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	if h.pageNo != rid.(RID).pageNo {
		return GoDBError{TupleNotFoundError, "Page does not match record ID"}
	}

	if h.getNumUsedSlots() == 0 {
		return GoDBError{TupleNotFoundError, "No entries to delete"}
	}

	if rid.(RID).slotNo < 0 || rid.(RID).slotNo >= h.totalSlots {
		return GoDBError{TupleNotFoundError, "Slot in record ID out of range"}
	}

	tuple := h.data[rid.(RID).slotNo]

	if tuple == nil {
		return GoDBError{TupleNotFoundError, "No tuple at specified slot"}
	}

	// the deletion
	h.data[rid.(RID).slotNo] = nil
	h.usedSlots--

	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	// TODO: some code goes here
	return nil //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)

	usedSlotsWriteError := binary.Write(buffer, binary.LittleEndian, int32(h.usedSlots))

	if usedSlotsWriteError != nil {
		return nil, usedSlotsWriteError
	}

	totalSlotsWriteError := binary.Write(buffer, binary.LittleEndian, int32(h.totalSlots))

	if totalSlotsWriteError != nil {
		return nil, totalSlotsWriteError
	}

	for _, tuple := range h.data {
		if tuple == nil {
			// just write bytesPerTuple zeroes
			zeroes := make([]byte, h.bytesPerTuple)

			zeroesWriteError := binary.Write(buffer, binary.LittleEndian, zeroes)

			if zeroesWriteError != nil {
				return nil, zeroesWriteError
			}
		} else {
			tupleWriteError := tuple.writeTo(buffer)

			if tupleWriteError != nil {
				return nil, tupleWriteError
			}
		}
	}

	// pad until it's finally PageSize
	if h.extraBytes > 0 {
		endZeroes := make([]byte, h.extraBytes)

		endZeroesWriteError := binary.Write(buffer, binary.LittleEndian, endZeroes)

		if endZeroesWriteError != nil {
			return nil, endZeroesWriteError
		}
	}

	return buffer, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	data := make([]byte, len(buf.Bytes()))

	readError := binary.Read(bytes.NewReader(buf.Bytes()), binary.LittleEndian, data)

	if readError != nil {
		return readError
	}

	h.usedSlots = int(binary.LittleEndian.Uint32(data[:4]))
	h.totalSlots = int(binary.LittleEndian.Uint32(data[4:8]))

	readIndex := 8

	const RemPageSize = PageSize - 8

	// the reverse of how we figured out total slots in the constructor
	h.bytesPerTuple = RemPageSize / h.totalSlots

	for slotIndex := 0; readIndex+h.bytesPerTuple <= PageSize; readIndex, slotIndex = readIndex+h.bytesPerTuple, slotIndex+1 {
		tupleBytes := data[readIndex : readIndex+h.bytesPerTuple]

		isTuple := false

		for _, tupleByte := range tupleBytes {
			if tupleByte > 0 {
				isTuple = true

				break
			}
		}

		if isTuple {
			tupleBuffer := bytes.NewBuffer(tupleBytes)

			var tupleReadError error

			h.data[slotIndex], tupleReadError = readTupleFrom(tupleBuffer, h.desc)

			if tupleReadError != nil {
				return tupleReadError
			}

			h.data[slotIndex].Rid = RID{h.pageNo, slotIndex}
		}
	}

	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	slotIndex := 0

	return func() (*Tuple, error) {
		// find next non-empty space
		for ; p.data[slotIndex] != nil; slotIndex++ {
		}

		if slotIndex >= p.totalSlots {
			return nil, nil
		}

		slotIndex++

		return p.data[slotIndex], nil
	}
}
