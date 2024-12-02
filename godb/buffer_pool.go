package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
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
	bufferPool          *BufferPool
}

func newPageStatus(bufferPool *BufferPool) *PageStatus {
	return &PageStatus{make(map[TransactionID]bool), NullTransactionID,
		bufferPool}
}

func (ps *PageStatus) requestSharedLock(tid TransactionID) bool {
	// println("shared lock being requested by", tid)

	// deny lock if anyone holds exclusive lock except me, grant otherwise
	if ps.exclusiveLockHolder != NullTransactionID && ps.exclusiveLockHolder != tid {
		// println("shared lock denied due to exclusive lock being occupied by", ps.exclusiveLockHolder)

		ps.bufferPool.addDependence(tid, ps.exclusiveLockHolder)

		return false
	} else {
		// testing membership, altho testing value would work too
		_, hasSharedLock := ps.sharedLockHolders[tid]

		if !hasSharedLock {
			ps.sharedLockHolders[tid] = true
		}

		return true
	}
}

func (ps *PageStatus) requestExclusiveLock(tid TransactionID) bool {
	// println("exclusive lock being requested by", tid)
	grant := true

	// deny lock if anyone holds exclusive lock except me
	if ps.exclusiveLockHolder != NullTransactionID && ps.exclusiveLockHolder != tid {
		// println("exclusive lock denied due to exclusive lock being occupied by", ps.exclusiveLockHolder)

		ps.bufferPool.addDependence(tid, ps.exclusiveLockHolder)

		grant = false
	}

	// i'm p sure this gets the number of valid entries in map
	sharedLockHolderCount := len(ps.sharedLockHolders)

	// grant if either nobody holds the shared lock or only i hold the shared lock
	if sharedLockHolderCount > 1 || (sharedLockHolderCount == 1 && !ps.sharedLockHolders[tid]) {
		for holder := range ps.sharedLockHolders {
			ps.bufferPool.addDependence(tid, holder)
		}

		grant = false
	}

	if grant {
		ps.exclusiveLockHolder = tid
	}

	// println("exclusive lock denied due to shared lock being occupied by", sharedLockHolderCount, "processes")

	return grant
}

func (ps *PageStatus) releaseSharedLock(tid TransactionID) {
	// no restrictions on releasing shared lock; you can always do it
	// testing by membership altho testing for value would've also worked
	_, hasSharedLock := ps.sharedLockHolders[tid]

	if hasSharedLock {
		// delete from the set by deleting from the map
		delete(ps.sharedLockHolders, tid)

		// println("shared lock released by ", tid)
		// println(len(ps.sharedLockHolders), "shared lock holders")
	}
}

func (ps *PageStatus) releaseExclusiveLock(tid TransactionID) {
	// no restrictions on releasing exclusive lock; you can always do it
	if ps.exclusiveLockHolder == tid {
		ps.exclusiveLockHolder = NullTransactionID

		// println("exclusive lock released by ", tid)
	}
}

func (ps *PageStatus) releaseLocks(tid TransactionID) {
	ps.releaseSharedLock(tid)
	ps.releaseExclusiveLock(tid)

	// disconnect all dependencies on me
	for possibleDepender := range ps.bufferPool.dependencies {
		ps.bufferPool.removeDependence(possibleDepender, tid)
	}

	// i no longer depend on anyone
	delete(ps.bufferPool.dependencies, tid)
}

type BufferPool struct {
	pages              map[any]Page
	maxPages           int
	logFile            *LogFile
	poolMutex          *sync.Mutex
	pageStatuses       map[Page]*PageStatus
	activeTransactions map[TransactionID]bool
	dependencies       map[TransactionID]map[TransactionID]bool

	// the transactions that are currently running. This is a set, so the value
	// is not important

	// TODO: some code goes here
}

// NewBufferPool Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{make(map[any]Page), numPages, nil, new(sync.Mutex),
		make(map[Page]*PageStatus), make(map[TransactionID]bool),
		make(map[TransactionID]map[TransactionID]bool)}, nil
}

func (bp *BufferPool) addDependence(depender TransactionID, dependsOn TransactionID) {
	// make submap if it didn't exist already
	if bp.dependencies[depender] == nil {
		bp.dependencies[depender] = make(map[TransactionID]bool)
	}

	bp.dependencies[depender][dependsOn] = true
}

func (bp *BufferPool) removeDependence(depender TransactionID, dependsOn TransactionID) {
	delete(bp.dependencies[depender], dependsOn)
}

func (bp *BufferPool) checkCycleDfs(tid TransactionID, temporaryVisited map[TransactionID]bool,
	permanentVisited map[TransactionID]bool) bool {
	if permanentVisited[tid] {
		// already visited this and found no cycle
		return false
	}
	if temporaryVisited[tid] {
		// graph has cycle
		return true
	}

	temporaryVisited[tid] = true

	for dependsOn := range bp.dependencies[tid] {
		hasCycle := bp.checkCycleDfs(dependsOn, temporaryVisited, permanentVisited)

		if hasCycle {
			return true
		}
	}

	permanentVisited[tid] = true

	// no cycle detected (yet)
	return false
}

func (bp *BufferPool) checkCycle(tid TransactionID) bool {
	temporaryVisited := make(map[TransactionID]bool)
	permanentVisited := make(map[TransactionID]bool)

	return bp.checkCycleDfs(tid, temporaryVisited, permanentVisited)
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
func (bp *BufferPool) tidIsRunning(tid TransactionID) bool {
	return bp.activeTransactions[tid]
}

func (bp *BufferPool) releaseLocks(tid TransactionID) {
	for _, status := range bp.pageStatuses {
		// these methods won't do anything if there is no lock so i can just call them without checking
		status.releaseSharedLock(tid)
		status.releaseExclusiveLock(tid)
	}
}

// AbortTransaction Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
// TODO: some code goes here : func (bp *BufferPool) AbortTransaction(tid TransactionID)
func (bp *BufferPool) AbortTransaction(tid TransactionID) error {
	// println("aborting...")

	bp.poolMutex.Lock()
	defer bp.poolMutex.Unlock()

	if !bp.tidIsRunning(tid) {
		return GoDBError{IllegalTransactionError, fmt.Sprintf("Cannot abort transation %d as "+
			"it is not running", tid)}
	}

	for pageId, page := range bp.pages {
		status := bp.pageStatuses[page]

		// delete page if i am writer and page is dirty
		if status.exclusiveLockHolder == tid && page.isDirty() {
			delete(bp.pages, pageId)
		}
	}

	bp.releaseLocks(tid)

	delete(bp.activeTransactions, tid)

	// println("aborted")

	return nil
}

// CommitTransaction Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
// TODO: some code goes here : func (bp *BufferPool) CommitTransaction(tid TransactionID)
func (bp *BufferPool) CommitTransaction(tid TransactionID) error {
	// println("committing...")

	bp.poolMutex.Lock()
	defer bp.poolMutex.Unlock()

	if !bp.tidIsRunning(tid) {
		return GoDBError{IllegalTransactionError, fmt.Sprintf("Cannot commit transation %d as "+
			"it is not running", tid)}
	}

	for page, status := range bp.pageStatuses {
		// flush page if i am writer and page is dirty
		if status.exclusiveLockHolder == tid && page.isDirty() {
			pageFlushError := page.getFile().flushPage(page)

			if pageFlushError != nil {
				return pageFlushError
			}

			page.setDirty(tid, false)
		}
	}

	bp.releaseLocks(tid)

	delete(bp.activeTransactions, tid)

	// println("committed")

	return nil
}

// BeginTransaction Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
// TODO: some code goes here: func (bp *BufferPool) BeginTransaction(tid TransactionID) error
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// println("begin transaction...")

	bp.poolMutex.Lock()
	defer bp.poolMutex.Unlock()

	if bp.tidIsRunning(tid) {
		return GoDBError{IllegalTransactionError, fmt.Sprintf("Cannot begin transaction %d as "+
			"it is already running", tid)}
	}

	bp.activeTransactions[tid] = true

	// println("began")

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

func (bp *BufferPool) deadlockCancel(tid TransactionID) {

}

// GetPage Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
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
		status = newPageStatus(bp)

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
		cycleExists := bp.checkCycle(tid)

		bp.poolMutex.Unlock()

		if cycleExists {
			abortError := bp.AbortTransaction(tid)

			if abortError != nil {
				return nil, abortError
			}

			return nil, GoDBError{DeadlockError, fmt.Sprintf("Deadlock detected; aborting "+
				"transaction %d", tid)}
		}

		// time.Sleep(10)

		bp.poolMutex.Lock()
	}

	// release pool mutex a final time
	bp.poolMutex.Unlock()

	return pg, nil
}
