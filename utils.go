package main

import (
	"crypto/sha256"
	"strconv"
)

// DNSQueryBuffer buffers DNS queries.
type DNSQueryBuffer struct {
	buckets              []map[[sha256.Size]byte]bool // The buckets contain a set of hashes. (https://yourbasic.org/golang/implement-set/)
	currentBucketPointer int                          // The bucket where the hashes are currently written to.
}

func newDNSQueryBuffer(windowSize int) *DNSQueryBuffer {
	b := make([]map[[sha256.Size]byte]bool, windowSize)
	for i := range b {
		b[i] = make(map[[sha256.Size]byte]bool)
	}
	return &DNSQueryBuffer{
		buckets:              b,
		currentBucketPointer: 0,
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
	db.buckets[db.currentBucketPointer][queryToHash(queryID, queryPort, qname, qtype)] = true
}

// run starts the sliding window mechanism where old entries
// that have not been used (i.e found through IsInBuffer) are deleted.
func (db *DNSQueryBuffer) run() {}

// moveCurrentBucketPointer moves the currentBucketPointer by one in the negative direction
// and clears the new current bucket's map.
func (db *DNSQueryBuffer) moveCurrentBucketPointer() {
	db.currentBucketPointer = (db.currentBucketPointer - 1 + len(db.buckets)) % len(db.buckets)
	db.buckets[db.currentBucketPointer] = make(map[[sha256.Size]byte]bool)
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
