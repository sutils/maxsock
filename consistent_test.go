package maxsock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"
)

type ConsistentCallback struct {
	wg sync.WaitGroup
}

func (c *ConsistentCallback) OnAccept(cl *ConsistentListener, conn io.ReadWriteCloser, option *AuthOption, offset int) (err error) {
	go c.runConn(conn, offset)
	return
}

func (c *ConsistentCallback) runConn(conn io.ReadWriteCloser, offset int) {
	buf := make([]byte, 1024)
	for {
		readed, err := conn.Read(buf)
		if err != nil {
			break
		}
		if string(buf[offset:readed]) != "1234567890" {
			panic("error")
		}
		c.wg.Done()
	}
}

func TestConsistentTCP(t *testing.T) {
	ShowLog = 1
	callback := &ConsistentCallback{wg: sync.WaitGroup{}}
	listener := NewConsistentListener(1024, 64, 8, 1024, 0, callback)
	listener.Acceptor.AuthKey["abc"] = "123"
	err := listener.ListenTCP("tcp", ":7233")
	if err != nil {
		t.Error(err)
		return
	}
	connector := NewConsistentConnector("abc", "123", 1024, 64, 8, 1024, 0)
	_, conn, _, err := connector.DailTCP("tcp", ":0", "localhost:7233", 0, nil)
	if err != nil {
		t.Error(err)
		return
	}
	callback.wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			buf := make([]byte, 18)
			copy(buf[8:], []byte("1234567890"))
			_, err := conn.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
	callback.wg.Wait()
}

func TestConsistentTCP2(t *testing.T) {
	ShowLog = 1
	callback := &ConsistentCallback{wg: sync.WaitGroup{}}
	listener := NewConsistentListener(1024, 64, 8, 1024, 0, callback)
	listener.Acceptor.AuthKey["abc"] = "123"
	err := listener.ListenTCP("tcp", ":7233")
	if err != nil {
		t.Error(err)
		return
	}
	connector := NewConsistentConnector("abc", "123", 1024, 64, 8, 1024, 0)
	_, conn, _, err := connector.DailTCP("tcp", ":0", "localhost:7233", 0, nil)
	if err != nil {
		t.Error(err)
		return
	}
	callback.wg.Add(100)
	for i := 0; i < 50; i++ {
		buf := make([]byte, 18)
		copy(buf[8:], []byte("1234567890"))
		_, err := conn.Write(buf)
		if err != nil {
			panic(err)
		}
	}
	for reader := range conn.(*ConsistentReadWriter).ConsistentReader.Raw.(*BindedReadWriter).BindedReader.binded {
		reader.Close()
	}
	time.Sleep(time.Second)
	for i := 0; i < 50; i++ {
		buf := make([]byte, 18)
		copy(buf[8:], []byte("1234567890"))
		_, err := conn.Write(buf)
		if err != nil {
			panic(err)
		}
	}
	callback.wg.Wait()
}

func TestConsistentUDP(t *testing.T) {
	ShowLog = 1
	callback := &ConsistentCallback{wg: sync.WaitGroup{}}
	listener := NewConsistentListener(1024, 64, 8, 1024, 0, callback)
	listener.Acceptor.AuthKey["abc"] = "123"
	err := listener.ListenUDP("udp", ":7233")
	if err != nil {
		t.Error(err)
		return
	}
	connector := NewConsistentConnector("abc", "123", 1024, 64, 8, 1024, 0)
	_, conn, _, err := connector.DailUDP("udp", ":0", "localhost:7233", 0, nil)
	if err != nil {
		t.Error(err)
		return
	}
	callback.wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			buf := make([]byte, 18)
			copy(buf[8:], []byte("1234567890"))
			_, err := conn.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
	callback.wg.Wait()
}

func TestConsistentReadWriterBasic(t *testing.T) {
	bindedReader, bindedWriter, err := pipeBindedReadWriter()
	if err != nil {
		return
	}
	consWriter := NewConsistentWriter(bindedWriter, 1024, 4)
	// consWriter.Copy = true
	consReader := NewConsistentReader(consWriter, bindedReader, 1024, 100, 1024, 4)
	//
	wg := sync.WaitGroup{}
	wg.Add(100)
	go func() {
		var idx uint16
		var readIdx uint16
		for {
			buf := make([]byte, 1024)
			readed, err := consReader.Read(buf)
			if err != nil {
				panic(err)
			}
			readIdx = binary.BigEndian.Uint16(buf[6:])
			if idx != readIdx {
				panic(fmt.Sprintf("%v-%v", idx, readIdx))
			}
			if string(buf[8:readed]) != "1234567890" {
				panic(string(buf[8:readed]))
			}
			idx++
			wg.Done()
		}
	}()
	for i := 0; i < 100; i++ {
		go func() {
			buf := make([]byte, 18)
			copy(buf[8:], []byte("1234567890"))
			_, err := consWriter.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	fmt.Println("--->waiting done...")
	for len(consWriter.queue) > 0 {
		time.Sleep(time.Second)
	}
}

func pipeConsistentReadWriter() (connA, connB *AutoCloseReadWriter, err error) {
	bindedReaderA, bindedWriterA, err := pipeBindedReadWriter()
	if err != nil {
		return
	}
	bindedReaderB, bindedWriterB, err := pipeBindedReadWriter()
	if err != nil {
		return
	}
	consWriterA := NewConsistentWriter(bindedWriterA, 10240, 4)
	consWriterB := NewConsistentWriter(bindedWriterB, 10240, 4)
	// consWriter.Copy = true
	consReaderA := NewConsistentReader(consWriterA, bindedReaderB, 1024, 100, 10240, 4)
	consReaderB := NewConsistentReader(consWriterB, bindedReaderA, 1024, 100, 10240, 4)
	// consWriter.Copy = true
	connA = NewAutoCloseReadWriter(consReaderA, consWriterA)
	connB = NewAutoCloseReadWriter(consReaderB, consWriterB)
	return
}

func TestConsistentReadWriterIndex(t *testing.T) {
	connA, connB, err := pipeConsistentReadWriter()
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("A:%p,B:%p\n", connA.Reader, connB.Reader)
	//
	wg := sync.WaitGroup{}
	wg.Add(10000)
	go func() {
		var idx uint16
		var readIdx uint16
		for {
			buf := make([]byte, 1024)
			readed, err := connB.Read(buf)
			if err != nil {
				panic(err)
			}
			readIdx = binary.BigEndian.Uint16(buf[6:])
			if idx != readIdx {
				panic(fmt.Sprintf("%v-%v", idx, readIdx))
			}
			if string(buf[8:readed]) != "1234567890" {
				panic(string(buf[8:readed]))
			}
			idx++
			wg.Done()
		}
	}()
	for i := 0; i < 10000; i++ {
		go func() {
			buf := make([]byte, 18)
			copy(buf[8:], []byte("1234567890"))
			_, err := connA.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	fmt.Println("--->waiting done...")
	time.Sleep(time.Second)
}

func TestConsistentReadWriterDataOrder(t *testing.T) {
	connA, connB, err := pipeConsistentReadWriter()
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("A:%p,B:%p\n", connA.Reader, connB.Reader)
	//
	wg := sync.WaitGroup{}
	wg.Add(100)
	go func() {
		var idx uint16
		var readIdx uint16
		for {
			buf := make([]byte, 1024)
			readed, err := connB.Read(buf)
			if err != nil {
				panic(err)
			}
			readIdx = binary.BigEndian.Uint16(buf[6:])
			if idx != readIdx {
				panic(fmt.Sprintf("%v-%v", idx, readIdx))
			}
			val, err := strconv.ParseInt(string(buf[8:readed]), 10, 64)
			if err != nil {
				panic(err)
			}
			if uint16(val) != idx {
				panic("error")
			}
			idx++
			wg.Done()
		}
	}()
	for i := 0; i < 100; i++ {
		buf := bytes.NewBuffer(nil)
		buf.Write(make([]byte, 8))
		fmt.Fprintf(buf, "%v", i)
		_, err := connA.Write(buf.Bytes())
		if err != nil {
			panic(err)
		}
	}
	wg.Wait()
	fmt.Println("--->waiting done...")
	time.Sleep(time.Second)
}

func TestConsistentReadWriterCallback(t *testing.T) {
	connA, connB, err := pipeConsistentReadWriter()
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("A:%p,B:%p\n", connA.Reader, connB.Reader)
	//
	wg := sync.WaitGroup{}
	wg.Add(100)
	go func() {
		var idx uint16
		var readIdx uint16
		for {
			buf := make([]byte, 1024)
			readed, err := connB.Read(buf)
			if err != nil {
				panic(err)
			}
			readIdx = binary.BigEndian.Uint16(buf[6:])
			if idx != readIdx {
				panic(fmt.Sprintf("%v-%v", idx, readIdx))
			}
			val, err := strconv.ParseInt(string(buf[8:readed]), 10, 64)
			if err != nil {
				panic(err)
			}
			if uint16(val) != idx {
				panic("error")
			}
			_, err = connB.Write(buf[:readed])
			if err != nil {
				panic(err)
			}
			idx++
		}
	}()
	go func() {
		var idx uint16
		var readIdx uint16
		for {
			buf := make([]byte, 1024)
			readed, err := connA.Read(buf)
			if err != nil {
				panic(err)
			}
			readIdx = binary.BigEndian.Uint16(buf[6:])
			if idx != readIdx {
				panic(fmt.Sprintf("%v-%v", idx, readIdx))
			}
			val, err := strconv.ParseInt(string(buf[8:readed]), 10, 64)
			if err != nil {
				panic(err)
			}
			if uint16(val) != idx {
				panic("error")
			}
			idx++
			wg.Done()
		}
	}()
	for i := 0; i < 100; i++ {
		buf := bytes.NewBuffer(nil)
		buf.Write(make([]byte, 8))
		fmt.Fprintf(buf, "%v", i)
		_, err := connA.Write(buf.Bytes())
		if err != nil {
			panic(err)
		}
	}
	wg.Wait()
	fmt.Println("--->waiting done...")
	time.Sleep(time.Second)
}

func BenchmarkConsistentReadWriterPipe(b *testing.B) {
	connA, connB, err := pipeConsistentReadWriter()
	if err != nil {
		b.Error(err)
		return
	}
	fmt.Printf("A:%p,B:%p\n", connA.Reader, connB.Reader)
	go func() {
		var idx uint16
		var readIdx uint16
		for {
			buf := make([]byte, 1024)
			readed, err := connB.Read(buf)
			if err != nil {
				panic(err)
			}
			readIdx = binary.BigEndian.Uint16(buf[6:])
			if idx != readIdx {
				panic(fmt.Sprintf("%v-%v", idx, readIdx))
			}
			if string(buf[8:readed]) != "1234567890" {
				panic(string(buf[8:readed]))
			}
			idx++
		}
	}()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := make([]byte, 18)
			copy(buf[8:], []byte("1234567890"))
			_, err := connA.Write(buf)
			if err != nil {
				panic(err)
			}
		}
	})
}
