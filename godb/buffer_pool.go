package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

// RWPerm Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	pages    map[any]Page
	maxPages int
	logFile  *LogFile

	// the transactions that are currently running. This is a set, so the value
	// is not important

	// TODO: some code goes here
}

// NewBufferPool Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{make(map[any]Page), numPages, nil}, nil
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

	return nil, fmt.Errorf("GetPage not implemented") //replace it
	// TODO: some code goes here
}
