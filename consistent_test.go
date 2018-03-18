package maxsock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestConsistentReadWriterBasic(t *testing.T) {
	bindedReader, bindedWriter, err := pipeBindedReadWriter()
	if err != nil {
		return
	}
	consWriter := NewConsistentWriter(bindedWriter, 1024)
	consWriter.Offset = 4
	// consWriter.Copy = true
	consReader := NewConsistentReader(consWriter, bindedReader, 1024, 100, 1024)
	consReader.Offset = 4
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
	consWriterA := NewConsistentWriter(bindedWriterA, 10240)
	consWriterA.Offset = 4
	consWriterB := NewConsistentWriter(bindedWriterB, 10240)
	consWriterB.Offset = 4
	// consWriter.Copy = true
	consReaderA := NewConsistentReader(consWriterA, bindedReaderB, 1024, 100, 10240)
	consReaderA.Offset = 4
	consReaderB := NewConsistentReader(consWriterB, bindedReaderA, 1024, 100, 10240)
	consReaderB.Offset = 4
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
