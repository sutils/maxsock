package maxsock

import (
	"io"
	"sync"
	"testing"
)

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
