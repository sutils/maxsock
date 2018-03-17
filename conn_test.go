package maxsock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Centny/gwf/log"
)

func TestConn(t *testing.T) {
	err := runConnRW(true)
	if err != nil {
		t.Error(err)
		return
	}
}

var areaded, awrited uint32

func runConnRW(show bool) (err error) {
	// fmt.Println("--->")
	connA := NewConn(0, 8, 1024)
	go func() {
		buf := make([]byte, 1024)
		var err error
		var readed int
		// fmt.Println("A is started")
		for {
			readed, err = connA.Read(buf)
			if err != nil {
				fmt.Println(readed)
				panic(err)
			}
			atomic.AddUint32(&areaded, 1)
			// fmt.Printf("A read %v data\n", readed)
			go func() {
				_, err = connA.Write(buf[:readed])
				if err != nil {
					panic(err)
				}
				atomic.AddUint32(&awrited, 1)
			}()
			// fmt.Printf("A write %v data\n", readed)
		}
	}()
	connB := NewConn(0, 8, 1024)
	//
	xConA, xConB := channelPipe()
	connA.Bind(xConA)
	connB.Bind(xConB)
	//
	// yConA, yConB := channelPipe()
	// connA.Bind(yConA)
	// connB.Bind(yConB)
	//
	wg := sync.WaitGroup{}
	go func() {
		buf := make([]byte, 1024)
		var err error
		var readed int
		for {
			readed, err = connB.Read(buf)
			if err != nil {
				fmt.Println(readed)
				panic(err)
			}
			wg.Done()
		}
	}()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v int) {
			_, err = connB.Write([]byte(fmt.Sprintf("d%v", v)))
			if err != nil {
				panic(err)
			}
		}(i)
	}
	// if show {
	// 	go func() {
	// 		for {
	// 			time.Sleep(time.Second)
	// 			fmt.Printf("A: write(%v) recved(%v) send(%v) readed(%v) srecv(%v) writeChan(%v) readChan(%v)->%v,%v\n",
	// 				connA.writed, connA.recved, connA.send, connA.readed, connA.srecv, len(connA.writeChan), len(connA.readChan), connA.srecv+connA.writed, len(connA.writeBuf))
	// 			fmt.Printf("B: write(%v) recved(%v) send(%v) readed(%v) srecv(%v) writeChan(%v) readChan(%v)->%v,%v\n",
	// 				connB.writed, connB.recved, connB.send, connB.readed, connB.srecv, len(connB.writeChan), len(connB.readChan), connB.srecv+connB.writed, len(connB.writeBuf))
	// 			// fmt.Printf("--->\n write:%v\n recved:%v\n send:%v\n readed:%v\n areaded:%v\n awrited:%v\n")

	// 		}
	// 	}()
	// }
	wg.Wait()
	return
}

func BenchmarkCoon(b *testing.B) {
	log.SetWriter(ioutil.Discard)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			runConnRW(false)
		}
	})
}

func TestFrameReadWriter(t *testing.T) {
	reader, writer := io.Pipe()
	frameReader := NewFrameReader(reader)
	frameWriter := NewFrameWriter(writer)
	wg := sync.WaitGroup{}
	wg.Add(100)
	go func() {
		for {
			buf := make([]byte, 1024)
			readed, err := frameReader.Read(buf)
			if err != nil {
				panic(err)
			}
			if string(buf[4:readed]) != "1234567890" {
				panic(string(buf[4:readed]))
			}
			wg.Done()
		}
	}()
	buf := make([]byte, 14)
	copy(buf[4:], []byte("1234567890"))
	for i := 0; i < 100; i++ {
		go func() {
			_, err := frameWriter.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
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
	buf := make([]byte, 14)
	copy(buf[4:], []byte("1234567890"))
	for i := 0; i < 100; i++ {
		go func() {
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
