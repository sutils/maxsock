package maxsock

import (
	"io"
	"sync"
	"testing"
)

func pipeFrameReadWriter() (reader *FrameReader, writer *FrameWriter) {
	rawReader, rawWriter := io.Pipe()
	reader = NewFrameReader(rawReader, 0)
	writer = NewFrameWriter(rawWriter, 0)
	return
}

func TestBindedConn(t *testing.T) {
	acceptor := NewBindedAcceptor(1024, 1024, 3, 4)
	acceptor.AuthKey["abc"] = "123"
	connector := NewBindedConnector("abc", "123", 1024, 1024, 3, 4)
	var err error
	var back = &AuthOption{}
	var connA, connB *BindedReadWriter
	for i := 0; i < 3; i++ {
		readerA, writerB := pipeFrameReadWriter()
		readerB, writerA := pipeFrameReadWriter()
		go func() {
			_, connA, err = acceptor.Accept(NewAutoCloseReadWriter(readerA, writerA))
			if err != nil {
				panic(err)
			}
		}()
		back, connB, err = connector.Connect(back.Session, nil, NewAutoCloseReadWriter(readerB, writerB))
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
		frameReader := NewFrameReader(reader, 0)
		frameWriter := NewFrameWriter(writer, 0)
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
		frameReader := NewFrameReader(rawReader, 0)
		frameWriter := NewFrameWriter(rawWriter, 0)
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
