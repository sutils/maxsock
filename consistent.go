package maxsock

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/Centny/gwf/log"
)

var ShowLog int = 2

type ConsistentReadWriter struct {
	*ConsistentReader
	*ConsistentWriter
}

func NewConsistentReadWriter(channel io.ReadWriter, bufferSize, readChanSize int, queueMax uint16) (conn *ConsistentReadWriter) {
	writer := NewConsistentWriter(channel, queueMax)
	reader := NewConsistentReader(writer, channel, bufferSize, readChanSize, queueMax)
	conn = &ConsistentReadWriter{
		ConsistentReader: reader,
		ConsistentWriter: writer,
	}
	return
}

//ConsistentReaderEvent is the interface of ConsistentReader
type ConsistentReaderEvent interface {
	OnSendHeartbeat(c *ConsistentReader, current uint16, missing []uint16) (err error)
	OnRecvHeartbeat(c *ConsistentReader, current uint16, missing []uint16) (err error)
}

//ConsistentReader is the reader to process data to be consistent
//more 4 byte is required when process read buffer, real data is 4 byte offset from begin.
type ConsistentReader struct {
	Raw        io.Reader     //the raw reader.
	BufferSize int           //the buffer size of read runner.
	Offset     int           //the offset position of write buffer to append the consistent info(4 byte)
	QueueMax   uint16        //the max of queued data
	Heartbeat  time.Duration //the deplay of heartbeat.
	//
	readChan   chan []byte
	taskChan   chan []byte
	readErr    error
	running    bool
	event      ConsistentReaderEvent
	currentIdx uint16
	missing    []uint16
}

//NewConsistentReader is the creator of ConsistentReader by event handler, raw writer, buffer size, read cache size and the max of queued data
func NewConsistentReader(event ConsistentReaderEvent, raw io.Reader, bufferSize, readChanSize int, queueMax uint16) (reader *ConsistentReader) {
	reader = &ConsistentReader{
		Raw:        raw,
		readChan:   make(chan []byte, readChanSize),
		taskChan:   make(chan []byte, readChanSize),
		BufferSize: bufferSize,
		event:      event,
		running:    true,
		Heartbeat:  500 * time.Millisecond,
		QueueMax:   queueMax,
	}
	go reader.runRead()
	go reader.runTask()
	go reader.runHeartbeat()
	return
}

func (c *ConsistentReader) runHeartbeat() {
	for c.running {
		time.Sleep(c.Heartbeat)
		c.event.OnSendHeartbeat(c, c.currentIdx, c.missing)
	}
	c.running = false
}

func (c *ConsistentReader) runTask() {
	for c.running {
		rawBuf := <-c.taskChan
		buf := rawBuf[c.Offset:]
		if buf[0] == 10 {
			if len(buf) < 4 {
				c.readErr = fmt.Errorf("heartbeat data is invalid")
				break
			}
			missing := []uint16{}
			missingLen := (len(buf) - 4) / 2
			for i := 0; i < missingLen; i++ {
				missing = append(missing, binary.BigEndian.Uint16(buf[4+2*i:]))
			}
			c.event.OnRecvHeartbeat(c, binary.BigEndian.Uint16(buf[2:]), missing)
		} else if buf[0] == 20 {
			c.event.OnSendHeartbeat(c, c.currentIdx, c.missing)
		}
	}
}

func (c *ConsistentReader) runRead() {
	if ShowLog > 0 {
		log.D("ConsistentReader(%v) read runner is starting", c)
	}
	received := map[uint16][]byte{}
	var receivedMax, dmax uint16
	for c.running {
		rawBuf := make([]byte, c.BufferSize)
		readed, err := c.Raw.Read(rawBuf)
		if err != nil {
			c.readErr = err
			break
		}
		rawBuf = rawBuf[:readed]
		buf := rawBuf[c.Offset:readed]
		//
		if buf[0] > 0 { //heartbeat message received
			c.taskChan <- rawBuf
			continue
		}
		dataIdx := binary.BigEndian.Uint16(buf[2:])
		received[dataIdx] = rawBuf
		{ //for heartbeat missing data
			disVal := int64(dataIdx) - int64(c.currentIdx)
			if disVal < 0 {
				disVal += math.MaxUint16
			}
			if receivedMax < uint16(disVal) {
				receivedMax = uint16(disVal)
			}
			if dmax < dataIdx {
				dmax = dataIdx
			}
			missing := []uint16{}
			for i := uint16(0); i < receivedMax; i++ {
				if received[c.currentIdx+i] == nil {
					missing = append(missing, c.currentIdx+i)
				}
			}
			c.missing = missing
			// fmt.Println("--->", c.currentIdx, dataIdx, receivedMax, missing, dmax, adis)
			//98 98 4 [99 100 101] 98
			if uint16(len(missing)) > c.QueueMax/2 {
				taskBuf := make([]byte, c.Offset+1)
				taskBuf[c.Offset] = 20
				c.taskChan <- taskBuf
			}
		}
		{ //pipe data to read
			for c.running {
				data := received[c.currentIdx]
				if data == nil {
					break
				}
				c.readChan <- data
				delete(received, c.currentIdx)
				c.currentIdx++
				if receivedMax > 0 {
					receivedMax--
				}
			}
		}
	}
	close(c.readChan)
	c.running = false
	if ShowLog > 0 {
		log.D("ConsistentReader(%v) read runner is stopped", c)
	}
}

func (c *ConsistentReader) Read(p []byte) (n int, err error) {
	buf := <-c.readChan
	if buf == nil {
		err = c.readErr
	} else {
		copy(p, buf)
		n = len(buf)
	}
	return
}

func (c *ConsistentReader) String() string {
	return fmt.Sprintf("%p,%v,%v,%v", c, c.Offset, c.currentIdx, c.Heartbeat)
}

//ConsistentWriter is the writer to process data to be consistent
//more 4 byte is required when process write buffer, real data is 4 byte offset from begin.
type ConsistentWriter struct {
	Raw    io.Writer //the raw writer to write data
	Offset int       //the offset position of write buffer to append the consistent info(4 byte)
	Copy   bool      //copy buffer to quque after write, default is false
	//
	writeLck   chan int
	writeLimit chan int
	currentIdx uint16
	queue      map[uint16][]byte
	queueLck   chan int
	queuedIdx  uint16
	QueueMax   uint16
}

//NewConsistentWriter is the creator of ConsistentWriter by raw writer and the max of queued data
func NewConsistentWriter(raw io.Writer, queueMax uint16) (writer *ConsistentWriter) {
	writer = &ConsistentWriter{
		Raw:        raw,
		writeLck:   make(chan int, 1),
		writeLimit: make(chan int, queueMax),
		queue:      map[uint16][]byte{},
		queueLck:   make(chan int, 1),
		QueueMax:   queueMax,
	}
	writer.writeLck <- 1
	writer.queueLck <- 1
	for i := uint16(0); i < queueMax; i++ {
		writer.writeLimit <- 1
	}
	return
}

func (c *ConsistentWriter) Write(p []byte) (n int, err error) {
	<-c.writeLimit
	//
	//do idx
	<-c.writeLck
	buf := p[c.Offset:]
	buf[0], buf[1] = 0, 0
	currentIdx := c.currentIdx
	c.currentIdx++
	binary.BigEndian.PutUint16(buf[2:], currentIdx)
	c.writeLck <- 1
	//
	//do queue
	cached := p
	if c.Copy {
		cached = make([]byte, len(p))
		copy(cached, p)
	}
	<-c.queueLck
	c.queue[currentIdx] = cached
	c.queuedIdx = currentIdx + 1
	c.queueLck <- 1
	//
	//do write
	n, err = c.Raw.Write(p)
	return
}

//OnSendHeartbeat is the ConsistentReader event hanndler.
func (c *ConsistentWriter) OnSendHeartbeat(cr *ConsistentReader, current uint16, missing []uint16) (err error) {
	rawBuf := make([]byte, len(missing)*2+c.Offset+4)
	buf := rawBuf[c.Offset:]
	buf[0], buf[1] = 10, 0
	binary.BigEndian.PutUint16(buf[2:], current)
	for idx, dataIdx := range missing {
		binary.BigEndian.PutUint16(buf[4+2*idx:], dataIdx)
	}
	_, err = c.Raw.Write(rawBuf)
	if err != nil {
		log.D("ConsistentWriter(%v) send heartbeat message fail with %v", c, err)
	} else if ShowLog > 1 {
		log.D("ConsistentWriter(%v) send heartbeat message success with current:%v,missing:%v", c, current, missing)
	}
	return
}

//OnRecvHeartbeat is the ConsistentReader event hanndler.
func (c *ConsistentWriter) OnRecvHeartbeat(cr *ConsistentReader, current uint16, missing []uint16) (err error) {
	<-c.queueLck
	if ShowLog > 1 {
		log.D("ConsistentWriter(%v) recv heartbeat message success with current(local:%v,remote:%v),missing:%v,", c, c.queuedIdx, current, missing)
	}
	queueMax := current + c.QueueMax
	for dataIdx := range c.queue {
		if queueMax >= current {
			if dataIdx < current || dataIdx > queueMax {
				delete(c.queue, dataIdx)
				c.writeLimit <- 1
			}
		} else {
			if dataIdx > queueMax && dataIdx < current {
				delete(c.queue, dataIdx)
				c.writeLimit <- 1
			}
		}
	}
	if c.queuedIdx == current {
		c.queueLck <- 1
		return
	}
	allMissing := map[uint16]bool{}
	for _, m := range missing {
		allMissing[m] = true
	}
	for i := current; i < c.queuedIdx; i++ {
		if allMissing[i] {
			continue
		}
		allMissing[i] = true
	}
	allBuf := [][]byte{}
	for m := range allMissing {
		rawBuf := c.queue[m]
		if rawBuf == nil {
			err = fmt.Errorf("cache not found(logic error)")
			log.E("ConsistentWriter(%v) write missing message fail with cache(%v) not found on queue(%v),logic error", c, m, len(c.queue))
			break
		}
		allBuf = append(allBuf, rawBuf)
	}
	c.queueLck <- 1
	if len(allBuf) > 0 {
		log.D("ConsistentWriter(%v) will resend %v data", c, len(allBuf))
		for _, rawBuf := range allBuf {
			_, err = c.Raw.Write(rawBuf)
			if err != nil {
				log.D("ConsistentWriter(%v) write missing message fail with %v", c, err)
				break
			}
		}
		log.D("ConsistentWriter(%v) resend %v data done", c, len(allBuf))
	}
	return
}

func (c *ConsistentWriter) String() string {
	return fmt.Sprintf("%p,%v,%v,%v", c, c.Offset, c.currentIdx, c.Copy)
}
