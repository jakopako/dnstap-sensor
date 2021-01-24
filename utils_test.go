package main

import (
	"strconv"
	"testing"
	"time"
)

const (
	queryID    uint16 = 1
	queryPort  uint32 = 5000
	qname      string = "example.com"
	qtype      uint16 = 1
	queryID1   uint16 = 2
	queryPort1 uint32 = 3000
	qname1     string = "google.com"
	qtype1     uint16 = 28
)

// TestIsInBuffer0 tests whether a query can be added to the buffer
// and found by IsInBuffer. This test is entirely static, i.e.
// the mechanism that removes old queries is not running.
func TestIsInBuffer0(t *testing.T) {
	db := newDNSQueryBuffer(10)
	db.addQuery(queryID, queryPort, qname, qtype)
	if !db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query is not in buffer but should be.")
	}
}

// TestIsInBuffer1 is very similar to TestIsInBuffer0 but checks
// for the same DNS query a second time to verify that after checking
// once it's not in the buffer anymore.
func TestIsInBuffer1(t *testing.T) {
	db := newDNSQueryBuffer(10)
	db.addQuery(queryID, queryPort, qname, qtype)
	if !db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query is not in buffer but should be.")
	}
	if db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query is still in buffer but should not be anymore.")
	}
}

func TestIsInBuffer2(t *testing.T) {
	db := newDNSQueryBuffer(10)
	db.addQuery(queryID, queryPort, qname, qtype)
	db.addQuery(queryID1, queryPort1, qname1, qtype1)
	if !db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query with qname %s is not in buffer but should be.", qname)
	}
	if !db.isInBuffer(queryID1, queryPort1, qname1, qtype1) {
		t.Errorf("Query with qname %s is not in buffer but should be.", qname)
	}
	if db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query with qname %s is still in buffer but should not be anymore.", qname)
	}
	if db.isInBuffer(queryID1, queryPort1, qname1, qtype1) {
		t.Errorf("Query with qname %s is still in buffer but should not be anymore.", qname)
	}
}

func TestMoveCurrentBucketPointer1(t *testing.T) {
	db := newDNSQueryBuffer(5)
	for i := 1; i < 5; i++ {
		db.addQuery(queryID, queryPort, qname, qtype)
		for j := 1; j <= i; j++ {
			db.moveCurrentBucketPointer()
		}
		if !db.isInBuffer(queryID, queryPort, qname, qtype) {
			t.Errorf("Query is not in buffer but should be.")
		}
		if db.isInBuffer(queryID, queryPort, qname, qtype) {
			t.Errorf("Query is still in buffer but should not be anymore.")
		}
	}
}

func TestMoveCurrentBucketPointer2(t *testing.T) {
	db := newDNSQueryBuffer(5)
	db.addQuery(queryID, queryPort, qname, qtype)
	for j := 1; j <= 5; j++ {
		db.moveCurrentBucketPointer()
	}
	if db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query is still in buffer but should not be anymore.")
	}
}

func TestRunAndStop(t *testing.T) {
	db := newDNSQueryBuffer(5)
	db.start()
	if !db.running {
		t.Errorf("DNSQueryBuffer should be running.")
	}
	time.Sleep(3 * time.Second)
	if !db.running {
		t.Errorf("DNSQueryBuffer should be running.")
	}
	db.stop()
	if db.running {
		t.Errorf("DNSQueryBuffer should not be running anymore.")
	}
}

func TestRunStillInBuffer(t *testing.T) {
	db := newDNSQueryBuffer(5)
	db.start()
	time.Sleep(500 * time.Millisecond)
	db.addQuery(queryID, queryPort, qname, qtype)
	time.Sleep(3 * time.Second)
	if !db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query should still be in the buffer.")
	}
	db.stop()
}

func TestRunNotInBufferAnymore(t *testing.T) {
	db := newDNSQueryBuffer(5)
	db.start()
	time.Sleep(500 * time.Millisecond)
	db.addQuery(queryID, queryPort, qname, qtype)
	time.Sleep(5 * time.Second)
	if db.isInBuffer(queryID, queryPort, qname, qtype) {
		t.Errorf("Query shouldn't be in the buffer anymore.")
	}
	db.stop()
}

func BenchmarkAddQuery(b *testing.B) {
	db := newDNSQueryBuffer(5)
	db.start()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.addQuery(queryID, queryPort, strconv.Itoa(i), qtype)
	}
}

func BenchmarkIsInBuffer(b *testing.B) {
	db := newDNSQueryBuffer(5)
	db.start()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if db.isInBuffer(queryID, queryPort, qname, qtype) {
			b.Errorf("Query should not be in buffer.")
		}
	}
}

func BenchmarkAddQueryIsInBuffer(b *testing.B) {
	db := newDNSQueryBuffer(5)
	db.start()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.addQuery(queryID, queryPort, strconv.Itoa(i), qtype)
		if !db.isInBuffer(queryID, queryPort, strconv.Itoa(i), qtype) {
			b.Errorf("Query should be in buffer.")
		}
	}
}
