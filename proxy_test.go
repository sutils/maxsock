package maxsock

import (
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func TestNewConsistentListenerTransparentRunner(t *testing.T) {
	var l net.Listener
	go func() {
		l, _ = net.Listen("tcp", ":2822")
		for {
			conn, err := l.Accept()
			if err != nil {
				panic(err)
			}
			go io.Copy(conn, conn)
		}
	}()
	runner := NewConsistentListenerTransparentRunner(1024, FrameOffset+ConsistentOffset)
	runner.ACL["abc"] = []string{"^.*$"}
	listener := NewConsistentListener(1024, 64, 8, 1024, 0, runner)
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
	localRunner := NewConsistentTransparentRunner("abc", 10, 1024, FrameOffset+ConsistentOffset, conn)
	channel := NewEchoReadWriter()
	_, err = localRunner.Connect(channel, "tcp", "localhost:2822")
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 100; i++ {
		channel.writeChan <- []byte(fmt.Sprintf("%v\n", i))
		time.Sleep(1 * time.Millisecond)
	}
	channel.Close()
	time.Sleep(time.Second)
}

func TestConsistentTransparentRunner(t *testing.T) {
	var listener net.Listener
	go func() {
		listener, _ = net.Listen("tcp", ":2822")
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			go io.Copy(conn, conn)
		}
	}()
	rawConnA, rawConnB := pipeFrameReadWriter2()
	runnerA := NewConsistentTransparentRunner("abc", 10, 1024, 4, rawConnA)
	runnerA.ACL = []string{"^.*$"}
	runnerB := NewConsistentTransparentRunner("abc", 10, 1024, 4, rawConnB)
	channel := NewEchoReadWriter()
	_, err := runnerB.Connect(channel, "tcp", "localhost:2822")
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 100; i++ {
		channel.writeChan <- []byte(fmt.Sprintf("%v\n", i))
		time.Sleep(1 * time.Millisecond)
	}
	channel.Close()
	time.Sleep(time.Second)
	rawConnA.Close()
	rawConnB.Close()
	time.Sleep(time.Second)
	listener.Close()
}

type EchoReadWriter struct {
	writeChan chan []byte
}

func NewEchoReadWriter() (echo *EchoReadWriter) {
	echo = &EchoReadWriter{
		writeChan: make(chan []byte),
	}
	return
}

func (e *EchoReadWriter) Read(p []byte) (n int, err error) {
	buf := <-e.writeChan
	if buf == nil {
		err = fmt.Errorf("closed")
		return
	}
	copy(p, buf)
	n = len(buf)
	return
}

func (e *EchoReadWriter) Write(p []byte) (n int, err error) {
	n, err = os.Stdout.Write(p)
	return
}

func (e *EchoReadWriter) Close() (err error) {
	close(e.writeChan)
	return
}
