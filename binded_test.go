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

func pipeFrameReadWriter() (reader *FrameReader, writer *FrameWriter) {
	rawReader, rawWriter := io.Pipe()
	reader = NewFrameReader(rawReader)
	writer = NewFrameWriter(rawWriter)
	return
}

func TestBindedConn(t *testing.T) {
	acceptor := NewBindedAcceptor(1024, 1024, 3)
	acceptor.AuthKey["abc"] = "123"
	acceptor.Offset = 4
	connector := NewBindedConnector("abc", "123", 1024, 1024, 3)
	connector.Offset = 4
	var err error
	var session uint32
	var connA, connB *BindedReadWriter
	for i := 0; i < 3; i++ {
		readerA, writerB := pipeFrameReadWriter()
		readerB, writerA := pipeFrameReadWriter()
		go func() {
			connA, err = acceptor.Accept(NewAutoCloseReadWriter(readerA, writerA))
			if err != nil {
				panic(err)
			}
		}()
		session, connB, err = connector.Connect(session, NewAutoCloseReadWriter(readerB, writerB))
		if err != nil {
			panic(err)
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(100)
	go func() {
		for {
			buf := make([]byte, 1024)
			readed, err := connB.Read(buf)
			if err != nil {
				panic(err)
			}
			if string(buf[4:readed]) != "1234567890" {
				panic(string(buf[4:readed]))
			}
			wg.Done()
		}
	}()
	//
	for i := 0; i < 100; i++ {
		go func() {
			buf := make([]byte, 14)
			copy(buf[4:], []byte("1234567890"))
			_, err := connA.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
}

func TestBindedReadWriter(t *testing.T) {
	bindedReader := NewBindedReader(1024, 100, 3)
	bindedWriter := NewBindedWriter(3)
	for i := 0; i < 3; i++ {
		reader, writer := io.Pipe()
		frameReader := NewFrameReader(reader)
		frameWriter := NewFrameWriter(writer)
		err := bindedReader.Bind(NewNoneCloserReader(frameReader))
		if err != nil {
			t.Error(err)
			return
		}
		err = bindedWriter.Bind(NewNoneCloserWriter(frameWriter))
		if err != nil {
			t.Error(err)
			return
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(100)
	go func() {
		for {
			buf := make([]byte, 1024)
			readed, err := bindedReader.Read(buf)
			if err != nil {
				panic(err)
			}
			if string(buf[4:readed]) != "1234567890" {
				panic(string(buf[4:readed]))
			}
			wg.Done()
		}
	}()
	for i := 0; i < 100; i++ {
		go func() {
			buf := make([]byte, 14)
			copy(buf[4:], []byte("1234567890"))
			_, err := bindedWriter.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
}

type BindedReaderEventHandler struct {
}

func (b *BindedReaderEventHandler) OnRawDone(br *BindedReader, raw io.Reader, err error) {
	panic(err)
}

func pipeBindedReadWriter() (reader *BindedReader, writer *BindedWriter, err error) {
	bindedReader := NewBindedReader(1024, 100, 3)
	bindedReader.Event = &BindedReaderEventHandler{}
	bindedWriter := NewBindedWriter(3)
	for i := 0; i < 1; i++ {
		rawReader, rawWriter := io.Pipe()
		frameReader := NewFrameReader(rawReader)
		frameWriter := NewFrameWriter(rawWriter)
		err = bindedReader.Bind(NewNoneCloserReader(frameReader))
		if err != nil {
			return
		}
		err = bindedWriter.Bind(NewNoneCloserWriter(frameWriter))
		if err != nil {
			return
		}
	}
	reader, writer = bindedReader, bindedWriter
	return
}

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
