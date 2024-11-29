package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"sync"
)

// RWPerm Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type PageStatus struct {
	// map that is used as a set; all present values are true
	// so you can test for membership in the set by testing membership in the map or by testing value in the map
	sharedLockHolders   map[TransactionID]bool
	exclusiveLockHolder TransactionID
}

func newPageStatus() *PageStatus {
	return &PageStatus{make(map[TransactionID]bool), NullTransactionID}
}

func (ps *PageStatus) requestSharedLock(tid TransactionID) bool {
	// deny lock if anyone holds exclusive lock except me
	if ps.exclusiveLockHolder != NullTransactionID && ps.exclusiveLockHolder != tid {
		return false
	}

	// testing membership, altho testing value would work too
	_, hasSharedLock := ps.sharedLockHolders[tid]

	if !hasSharedLock {
		ps.sharedLockHolders[tid] = true
	}

	return true
}

func (ps *PageStatus) requestExclusiveLock(tid TransactionID) bool {
	// deny lock if anyone holds exclusive lock except me
	if ps.exclusiveLockHolder != NullTransactionID && ps.exclusiveLockHolder != tid {
		return false
	}

	// i'm p sure this gets the number of valid entries in map
	sharedLockHolderCount := len(ps.sharedLockHolders)

	// grant if either nobody holds the shared lock or only i hold the shared lock
	if sharedLockHolderCount == 0 {
		ps.exclusiveLockHolder = tid

		return true
	} else if sharedLockHolderCount == 1 && ps.sharedLockHolders[tid] {
		// tested by value above cuz it was just more convenient
		ps.exclusiveLockHolder = tid

		// TODO: maybe release the shared lock on upgrade?

		return true
	}

	return false
}

func (ps *PageStatus) releaseSharedLock(tid TransactionID) {
	// no restrictions on releasing shared lock; you can always do it
	// testing by membership altho testing for value would've also worked
	_, hasSharedLock := ps.sharedLockHolders[tid]

	if !hasSharedLock {
		// delete from the set by deleting from the map
		delete(ps.sharedLockHolders, tid)
	}
}

func (ps *PageStatus) releaseExclusiveLock(tid TransactionID) {
	// no restrictions on releasing exclusive lock; you can always do it
	if ps.exclusiveLockHolder == tid {
		ps.exclusiveLockHolder = NullTransactionID
	}
}

type BufferPool struct {
	pages        map[any]Page
	maxPages     int
	logFile      *LogFile
	poolMutex    *sync.Mutex
	pageStatuses map[Page]*PageStatus

	// the transactions that are currently running. This is a set, so the value
	// is not important

	// TODO: some code goes here
}

// NewBufferPool Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{make(map[any]Page), numPages, nil, new(sync.Mutex),
		make(map[Page]*PageStatus)}, nil
}

// FlushAllPages Testing method -- iterate through all pages in the buffer pool and flush them
// using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.pages {
		err := page.getFile().flushPage(page)

		if err != nil {
			return
		}
	}
}

// Testing method -- flush all dirty pages in the buffer pool and set them to
// clean. Does not need to be thread/transaction safe.
// TODO: some code goes here : func (bp *BufferPool) flushDirtyPages(tid TransactionID) error

// Returns true if the transaction is runing.
//
// Caller must hold the bufferpool lock.
// TODO: some code goes here : func (bp *BufferPool) tidIsRunning(tid TransactionID) bool

// AbortTransaction Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
// TODO: some code goes here : func (bp *BufferPool) AbortTransaction(tid TransactionID)
func (bp *BufferPool) AbortTransaction(tid TransactionID) {

}

// CommitTransaction Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
// TODO: some code goes here : func (bp *BufferPool) CommitTransaction(tid TransactionID)
func (bp *BufferPool) CommitTransaction(tid TransactionID) {

}

// BeginTransaction Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
// TODO: some code goes here: func (bp *BufferPool) BeginTransaction(tid TransactionID) error
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	return nil
}

// If necessary, evict clean page from the buffer pool. If all pages are dirty,
// return an error.
func (bp *BufferPool) evictPage() error {
	if len(bp.pages) < bp.maxPages {
		return nil
	}

	// evict first clean page
	for key, page := range bp.pages {
		if !page.isDirty() {
			delete(bp.pages, key)
			return nil
		}
	}

	return GoDBError{BufferPoolFullError, "all pages in buffer pool are dirty"}
}

// Returns true if the transaction is runing.
// TODO: some code goes here :func (bp *BufferPool) IsRunning(tid TransactionID) bool

// Loads the specified page from the specified DBFile, but does not lock it.
// TODO: some code goes here : func (bp *BufferPool) loadPage(file DBFile, pageNo int) (Page, error)

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	// TODO: some code goes here
	// get the pool mutex
	bp.poolMutex.Lock()

	hashCode := file.pageKey(pageNo)
	pg, ok := bp.pages[hashCode]
	if !ok {
		err := bp.evictPage()
		if err != nil {
			return nil, err
		}
		pg, err = file.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		bp.pages[hashCode] = pg
	}

	status, statusExists := bp.pageStatuses[pg]

	// create new page status for this page if one doesn't already exist
	if !statusExists {
		status = newPageStatus()

		bp.pageStatuses[pg] = status
	}

	var acquireMethod func(tid TransactionID) bool

	if perm == ReadPerm {
		// shared lock for read perm
		acquireMethod = status.requestSharedLock
	} else {
		// exclusive lock for write perm
		acquireMethod = status.requestExclusiveLock
	}

	// repeatedly try to request the shared or exclusive lock
	// release pool mutex after each failure and reacquire it before each attempt
	for !acquireMethod(tid) {
		bp.poolMutex.Unlock()
		bp.poolMutex.Lock()
	}

	// release pool mutex a final time
	bp.poolMutex.Unlock()

	return pg, nil
}
