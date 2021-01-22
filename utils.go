package main

import "container/list"

// DNSQueryBuffer buffers DNS queries.
type DNSQueryBuffer struct {
	buckets    *list.List
	windowSize int //in seconds
}

func newDNSQueryBuffer(windowSize int) *DNSQueryBuffer {
	return &DNSQueryBuffer{
		buckets:    list.New(),
		windowSize: windowSize,
	}
}

// addQuery adds a new DNS query to the DNSQueryBuffer.
func (db *DNSQueryBuffer) addQuery(queryID uint16, queryPort uint32, qname string, qtype uint16) {}

// run starts the sliding window mechanism where old entries
// that have not been used (i.e found through IsInBuffer) are deleted.
func (db *DNSQueryBuffer) run() {}

// isInBuffer checks whether the specified combination of a, b, c
// is in the buffer and if so deletes this specific entry.
func (db *DNSQueryBuffer) isInBuffer(queryID uint16, queryPort uint32, qname string, qtype uint16) bool {
	return false
}
