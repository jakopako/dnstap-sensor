package main

import (
	"crypto/sha256"
	"strconv"
	"time"
)

// DNSQueryBuffer buffers DNS queries.
type DNSQueryBuffer struct {
	buckets              []map[[sha256.Size]byte]bool // The buckets contain a set of hashes. (https://yourbasic.org/golang/implement-set/)
	currentBucketPointer int                          // The bucket where the hashes are currently written to.
	quit                 chan bool
	running              bool
}

func newDNSQueryBuffer(windowSize int) *DNSQueryBuffer {
	b := make([]map[[sha256.Size]byte]bool, windowSize)
	for i := range b {
		b[i] = make(map[[sha256.Size]byte]bool)
	}
	return &DNSQueryBuffer{
		buckets:              b,
		currentBucketPointer: 0,
		quit:                 make(chan bool),
		running:              false,
	}
}

func queryToHash(queryID uint16, queryPort uint32, qname string, qtype uint16) [sha256.Size]byte {
	s := strconv.FormatUint(uint64(queryID), 10) +
		strconv.FormatUint(uint64(queryPort), 10) +
		qname +
		strconv.FormatUint(uint64(qtype), 10)
	return sha256.Sum256([]byte(s))
}

// addQuery adds a new DNS query to the DNSQueryBuffer.
func (db *DNSQueryBuffer) addQuery(queryID uint16, queryPort uint32, qname string, qtype uint16) {
	// TODO: have some kind of lock that prevents adding a query while the currentBucketPointer
	// is being moved.
	db.buckets[db.currentBucketPointer][queryToHash(queryID, queryPort, qname, qtype)] = true
}

// start starts the sliding window mechanism where old entries
// that have not been used (i.e found through IsInBuffer) are deleted.
func (db *DNSQueryBuffer) start() {
	if !db.running {
		db.running = true
		go db.run()
	}
}

// stop stops the sliding window mechanism.
func (db *DNSQueryBuffer) stop() {
	if db.running {
		db.quit <- true
	}
}

func (db *DNSQueryBuffer) run() {
	for {
		select {
		case <-db.quit:
			db.running = false
			return
		default:
			go db.moveCurrentBucketPointer()
			time.Sleep(1 * time.Second)
		}
	}
}

// moveCurrentBucketPointer moves the currentBucketPointer by one in the negative direction
// and clears the new current bucket's map.
func (db *DNSQueryBuffer) moveCurrentBucketPointer() {
	// TODO: make sure this function can only be called once.
	//start := time.Now()
	// First delete the oldest bucket
	oldestBucketPointer := (db.currentBucketPointer - 1 + len(db.buckets)) % len(db.buckets)
	db.buckets[oldestBucketPointer] = make(map[[sha256.Size]byte]bool)
	// Then update the currentBucketPointer. Is a lock necessary for this operation?
	// What happens when a query is written to the DNSQueryBuffer (by addQuery) the exact
	// same moment when the pointer is updated?
	db.currentBucketPointer = oldestBucketPointer
	//elapsed := time.Since(start)
	//log.Printf("Moving pointer took %s", elapsed)
}

// isInBuffer checks whether the specified combination of a, b, c
// is in the buffer and if so deletes this specific entry.
func (db *DNSQueryBuffer) isInBuffer(queryID uint16, queryPort uint32, qname string, qtype uint16) bool {
	h := queryToHash(queryID, queryPort, qname, qtype)
	for i := db.currentBucketPointer; i < db.currentBucketPointer+len(db.buckets); i++ {
		j := i % len(db.buckets)
		if _, ok := db.buckets[j][h]; ok {
			delete(db.buckets[j], h)
			return true
		}
	}
	return false
}
