package maxsock

import (
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	runtime.GOMAXPROCS(8)
	go http.ListenAndServe(":2722", nil)
}

type channel struct {
	id   uint32
	buf  chan []byte
	pipe *channel
}

func (c *channel) String() string {
	return fmt.Sprintf("c%v", c.id)
}

func (c *channel) Read(p []byte) (n int, err error) {
	buf := <-c.pipe.buf
	copy(p, buf)
	n = len(buf)
	return
}

func (c *channel) Write(p []byte) (n int, err error) {
	buf := make([]byte, len(p))
	copy(buf, p)
	c.buf <- buf
	n = len(p)
	return
}

func (c *channel) Close() (err error) {
	return
}

var idx uint32

func channelPipe() (a, b io.ReadWriteCloser) {
	ca, cb := &channel{
		id:  atomic.AddUint32(&idx, 1),
		buf: make(chan []byte, 100),
	}, &channel{
		id:  atomic.AddUint32(&idx, 1),
		buf: make(chan []byte, 100),
	}
	ca.pipe, cb.pipe = cb, ca
	a, b = ca, cb
	return
}

type channel2 struct {
	io.Reader
	io.WriteCloser
}

func channelPipe2() (a, b io.ReadWriteCloser) {
	xPipeR, xPipeW := io.Pipe()
	yPipeR, yPipeW := io.Pipe()
	a = &channel2{
		Reader:      xPipeR,
		WriteCloser: yPipeW,
	}
	b = &channel2{
		Reader:      yPipeR,
		WriteCloser: xPipeW,
	}
	return
}

func TestChannelPipe(t *testing.T) {
	ca, cb := channelPipe2()
	go func() {
		buf := make([]byte, 1024)
		for {
			readed, _ := cb.Read(buf)
			cb.Write(buf[0:readed])
		}
	}()
	go func() {
		buf := make([]byte, 1024)
		for {
			readed, _ := ca.Read(buf)
			fmt.Printf("read->%v\n", string(buf[:readed]))
		}
	}()
	for i := 0; i < 100; i++ {
		ca.Write([]byte(fmt.Sprintf("val-%v", i)))
	}
	time.Sleep(time.Second)
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
