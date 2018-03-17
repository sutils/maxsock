package maxsock

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Centny/gwf/log"
)

var _cidx uint32

var ShowLog int = 2

const (
	//ConnSignalData is data transfer signal
	ConnSignalData = 0
	//ConnSignalRecv is data received signal
	ConnSignalRecv = 1
)

type RawChannel struct {
	io.ReadWriteCloser
	Closer chan int
}

func (r *RawChannel) Close() (err error) {
	r.Closer <- -1
	err = r.ReadWriteCloser.Close()
	return
}

func (r *RawChannel) String() string {
	return fmt.Sprintf("%v", r.ReadWriteCloser)
}

//Conn is an binded connect by multi channel.
type Conn struct {
	ID uint32
	// binded    map[io.ReadWriteCloser]chan int
	// bindedLck sync.RWMutex
	connPool chan io.ReadWriteCloser
	running  bool
	//
	writeBuf map[uint64][]byte
	writeIdx uint64
	// writeChan chan []byte
	writeLck *sync.Cond
	//
	readBuf  map[uint64][]byte
	readIdx  uint64
	readChan chan []byte
	readLck  sync.RWMutex
	//
	Session     uint32
	BufferLimit int
	BufferSize  int
	Delay       time.Duration
	//
	closeError error
	Userinfo   interface{}
}

//NewConn is creator by buffer limit and size.
//the max buffer memory of one connect will be caculated by bufferLimit*bufferSize*2.5
func NewConn(session uint32, bufferLimit, bufferSize int) (conn *Conn) {
	conn = &Conn{
		ID: atomic.AddUint32(&_cidx, 1),
		// binded:    map[io.ReadWriteCloser]chan int{},
		// bindedLck: sync.RWMutex{},
		connPool: make(chan io.ReadWriteCloser, 1024),
		//
		writeBuf: map[uint64][]byte{},
		// writeChan: make(chan []byte, bufferLimit),
		writeLck: sync.NewCond(&sync.RWMutex{}),
		//
		readBuf:  map[uint64][]byte{},
		readChan: make(chan []byte, bufferLimit),
		readLck:  sync.RWMutex{},
		//
		running:     true,
		Session:     session,
		BufferLimit: bufferLimit,
		BufferSize:  bufferSize,
		Delay:       10 * time.Millisecond,
		closeError:  fmt.Errorf("connection has be closed"),
	}
	return
}

//Bind one channel to Connection.
func (c *Conn) Bind(channel io.ReadWriteCloser) {
	// c.bindedLck.Lock()
	// defer c.bindedLck.Unlock()
	closer := make(chan int, 8)
	// c.binded[channel] = closer
	raw := &RawChannel{
		Closer:          closer,
		ReadWriteCloser: channel,
	}
	go c.runRead(raw)
	// go c.runWrite(closer, channel)
	go c.runResend()
	c.connPool <- raw
}

func (c *Conn) runResend() {
	log.D("%v start resend data", c)
	for c.running {
		c.writeLck.L.Lock()
		for _, buf := range c.writeBuf {
			c.rawWrite(buf)
		}
		c.writeLck.L.Unlock()
		time.Sleep(c.Delay)
	}
}

func (c *Conn) runRead(channel io.ReadWriteCloser) {
	log.D("%v start run read on channel(%v)", c, channel)
	for c.running {
		var readed, required uint32
		var dataIdx uint64
		buf := make([]byte, c.BufferSize)
		readingBuf := buf
		for {
			oneReaded, err := channel.Read(readingBuf)
			if err != nil {
				channel.Close()
				break
			}
			readed += uint32(oneReaded)
			if required < 1 {
				if readed < 13 { //need more head
					readingBuf = buf[readed:]
					continue
				}
				dataIdx = binary.BigEndian.Uint64(buf[5:])
				required = binary.BigEndian.Uint32(buf[1:])
			}
			if required > readed { //need more data
				readingBuf = buf[readed:]
				continue
			}
			//one frame readed
			break
		}
		if ShowLog > 1 {
			log.D("%v read %v data on raw(%v)", c, readed, channel)
		}
		if buf[0] == ConnSignalRecv {
			// log.D("%v done %v idx", c, dataIdx)
			c.writeLck.L.Lock()
			delete(c.writeBuf, dataIdx)
			c.writeLck.Signal()
			c.writeLck.L.Unlock()
			continue
		}
		//
		c.sendRecv(dataIdx)
		//
		c.readLck.Lock()
		c.readBuf[dataIdx] = buf[13:readed]
		for {
			buf := c.readBuf[c.readIdx]
			if buf == nil {
				break
			}
			c.readChan <- c.readBuf[c.readIdx]
			delete(c.readBuf, c.readIdx)
			c.readIdx++
		}
		c.readLck.Unlock()
	}
	log.D("%v run read is stopped on channel(%v)", c, channel)
}

// func (c *Conn) runWrite(closer chan int, channel io.ReadWriteCloser) {
// 	log.D("%v start run write on channel(%v)", c, channel)
// 	for c.running {
// 		select {
// 		case buf := <-c.writeChan:
// 			writed, err := channel.Write(buf)
// 			if err != nil || writed != len(buf) {
// 				c.writeChan <- buf //push back to queue
// 				return
// 			}
// 		case <-closer:
// 			channel.Close()
// 			return
// 		}
// 	}
// 	c.bindedLck.Lock()
// 	delete(c.binded, channel)
// 	c.bindedLck.Unlock()
// 	channel.Close()
// 	closer <- 1
// 	log.D("%v run write is stopped on channel(%v)", c, channel)
// }

func (c *Conn) sendRecv(idx uint64) {
	// log.D("%v send recv by idx %v", c, idx)
	buf := make([]byte, 14)
	buf[0] = ConnSignalRecv
	binary.BigEndian.PutUint64(buf[5:], idx)
	binary.BigEndian.PutUint32(buf[1:], 14)
	buf[13] = 0
	c.rawWrite(buf)
}

func (c *Conn) rawWrite(p []byte) (n int, err error) {
	raw := <-c.connPool
	n, err = raw.Write(p)
	if ShowLog > 1 {
		log.D("%v write %v data on raw(%v)", c, n, raw)
	}
	return
}

func (c *Conn) Write(p []byte) (n int, err error) {
	if !c.running {
		err = fmt.Errorf("connection is not running")
		return
	}
	bufferSize := len(p)
	if c.BufferSize < bufferSize {
		err = fmt.Errorf("data too large by limit %v, but %v", c.BufferSize, bufferSize)
		return
	}
	buf := make([]byte, len(p)+13)
	buf[0] = ConnSignalData
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)))
	copy(buf[13:], p)
	//
	c.writeLck.L.Lock()
	if len(c.writeBuf) >= c.BufferLimit {
		c.writeLck.Wait()
	}
	idx := c.writeIdx
	binary.BigEndian.PutUint64(buf[5:], idx)
	c.writeIdx++
	c.writeBuf[idx] = buf
	// log.D("%v send %v data", c, idx)
	c.writeLck.L.Unlock()
	// c.writeChan <- buf //push to queue
	c.rawWrite(buf)
	n = len(p)
	return
}

func (c *Conn) Read(p []byte) (n int, err error) {
	if !c.running {
		err = fmt.Errorf("connection is not running")
		return
	}
	bufferSize := len(p)
	if c.BufferSize < bufferSize {
		err = fmt.Errorf("buffer too smaller by limit %v, but %v", c.BufferSize, bufferSize)
		return
	}
	buf := <-c.readChan
	if buf == nil {
		err = c.closeError
		return
	}
	copy(p, buf)
	n = len(buf)
	return
}

//Close the connection.
func (c *Conn) Close() (err error) {
	// c.bindedLck.Lock()
	// defer c.bindedLck.Unlock()
	// c.running = false
	// closer := make(chan int, 1)
	// closer <- 1
	// for {
	// 	select {
	// 	case raw := <-c.connPool:
	// 		raw.Close()
	// 	case closer <- 1:
	// 		break
	// 	}
	// }
	// log.D("%v is closing", c)
	return
}

func (c *Conn) String() string {
	return fmt.Sprintf("Conn(%v)", c.ID)
}

func WriteFrame(w io.Writer, head byte, data []byte) (n int, err error) {
	buf := make([]byte, len(data)+5)
	buf[0] = head
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)))
	copy(buf[5:], data)
	n, err = w.Write(buf)
	fmt.Println(w, "write->", len(buf))
	return
}

func WriteJSON(w io.Writer, head byte, v interface{}) (n int, err error) {
	data, err := json.Marshal(v)
	if err == nil {
		n, err = WriteFrame(w, head, data)
	}
	return
}

type ConsistentReaderEvent interface {
	OnSendHeartbeat(c *ConsistentReader, current uint16, missing []uint16) (err error)
	OnRecvHeartbeat(c *ConsistentReader, current uint16, missing []uint16) (err error)
}

type ConsistentReader struct {
	Raw        io.Reader
	readChan   chan []byte
	taskChan   chan []byte
	readErr    error
	running    bool
	BufferSize int
	Offset     int
	QueueMax   int
	//
	event      ConsistentReaderEvent
	currentIdx uint16
	missing    []uint16
	Heartbeat  time.Duration
}

func NewConsistentReader(event ConsistentReaderEvent, raw io.Reader, bufferSize, readChanSize, queueMax int) (reader *ConsistentReader) {
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
		if len(c.missing) > 1000 {
			panic("xxxx->")
		}
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
	log.D("ConsistentReader(%v) read runner is starting", c)
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
			if len(missing) > c.QueueMax/2 {
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
	log.D("ConsistentReader(%v) read runner is stopped", c)
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

//4 byte offset
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

type BindedReaderEvent interface {
	OnRawDone(b *BindedReader, raw io.Reader, err error)
}

type BindedReader struct {
	BufferSize int
	Max        int
	Event      BindedReaderEvent
	readChan   chan []byte
	readErr    error
	binded     map[io.ReadCloser]int
	bindedLck  chan int
	running    bool
}

func NewBindedReader(bufferSize, readChanSize, max int) (reader *BindedReader) {
	reader = &BindedReader{
		BufferSize: bufferSize,
		Max:        max,
		readChan:   make(chan []byte, readChanSize),
		binded:     map[io.ReadCloser]int{},
		bindedLck:  make(chan int, 1),
		running:    true,
	}
	reader.bindedLck <- 1
	return
}

func (b *BindedReader) Bind(raw io.ReadCloser) (err error) {
	<-b.bindedLck
	if b.running {
		if len(b.binded) >= b.Max {
			err = fmt.Errorf("BindedReader bind channel is full with limit:%v, current:%v", b.Max, len(b.binded))
		} else {
			b.binded[raw] = 1
			go b.runRead(raw)
			log.D("BindedReader(%v) bind raw(%v) success", b, raw)
		}
	} else {
		err = fmt.Errorf("BindedReader is not running")
	}
	b.bindedLck <- 1
	return
}

func (b *BindedReader) runRead(raw io.ReadCloser) {
	log.D("BindedReader(%v) raw reader is starting", b)
	var readed int
	var err error
	for {
		buf := make([]byte, b.BufferSize)
		readed, err = raw.Read(buf)
		if err != nil {
			break
		}
		b.readChan <- buf[:readed]
	}
	<-b.bindedLck
	delete(b.binded, raw)
	b.bindedLck <- 1
	if b.Event != nil && b.running {
		b.Event.OnRawDone(b, raw, err)
	}
	log.D("BindedReader(%v) raw reader is done with error:%v", b, err)
}

func (b *BindedReader) Read(p []byte) (n int, err error) {
	buf := <-b.readChan
	if buf == nil {
		err = b.readErr
	} else {
		copy(p, buf)
		n = len(buf)
	}
	return
}

func (b *BindedReader) Close() (err error) {
	<-b.bindedLck
	b.running = false
	for raw := range b.binded {
		cerr := raw.Close()
		if cerr != nil {
			cerr = err
		}
	}
	b.bindedLck <- 1
	return
}

func (b *BindedReader) String() string {
	return fmt.Sprintf("%p,%v", b, b.Max)
}

type BindedWriter struct {
	Max        int
	binded     map[io.WriteCloser]int
	bindedLck  chan int
	bindedChan chan io.Writer
	running    bool
}

func NewBindedWriter(max int) (writer *BindedWriter) {
	writer = &BindedWriter{
		Max:        max,
		binded:     map[io.WriteCloser]int{},
		bindedLck:  make(chan int, 1),
		bindedChan: make(chan io.Writer, max),
		running:    true,
	}
	writer.bindedLck <- 1
	return
}

func (b *BindedWriter) Bind(raw io.WriteCloser) (err error) {
	<-b.bindedLck
	if b.running {
		if len(b.binded) >= b.Max {
			err = fmt.Errorf("BindedReader bind channel is full with limit:%v, current:%v", b.Max, len(b.binded))
		} else {
			b.binded[raw] = 1
			b.bindedChan <- raw
			log.D("BindedWriter(%v) bind raw(%v) success", b, raw)
		}
	} else {
		err = fmt.Errorf("BindeWriter is not running")
	}
	b.bindedLck <- 1
	return
}

func (b *BindedWriter) Write(p []byte) (n int, err error) {
	for b.running {
		raw := <-b.bindedChan
		if raw == nil {
			err = fmt.Errorf("BindedWriter has been closed")
			b.bindedLck <- 1
			break
		}
		n, err = raw.Write(p)
		if err == nil {
			b.bindedChan <- raw
			break
		}
	}
	return
}

func (b *BindedWriter) Close() (err error) {
	<-b.bindedLck
	b.running = false
	for raw := range b.binded {
		cerr := raw.Close()
		if cerr != nil {
			cerr = err
		}
		delete(b.binded, raw)
	}
	close(b.bindedChan)
	b.bindedLck <- 1
	return
}

func (b *BindedWriter) String() string {
	return fmt.Sprintf("%p,%v", b, b.Max)
}

//FrameReader impl the reader to read data as frame.
//for more info about FrameReader, see FrameReader.Read.
type FrameReader struct {
	Raw    io.Reader //the raw reader
	Offset int       //the begin position of saving frame length
}

//NewFrameReader is creator by raw reader.
func NewFrameReader(raw io.Reader) (reader *FrameReader) {
	reader = &FrameReader{
		Raw: raw,
	}
	return
}

//Read data as frame, 4 byte is required to save frame length on buffer p and it's begin position is FrameReader.Offset.
//so the total size of buffer is 4 byte more than the data size.
func (f *FrameReader) Read(p []byte) (n int, err error) {
	var readed, required uint32
	var once int
	buf := p
	readingBuf := buf[0 : 4+f.Offset]
	for {
		once, err = f.Raw.Read(readingBuf)
		if err != nil {
			break
		}
		readed += uint32(once)
		if required < 1 {
			if readed < 4+uint32(f.Offset) { //need more head
				readingBuf = buf[readed : 4+f.Offset]
				continue
			}
			required = binary.BigEndian.Uint32(buf[f.Offset:])
			if required > uint32(len(p)) {
				err = fmt.Errorf("bad frame size %v, buffer size is %v", required, len(p))
				break
			}
		}
		if required > readed { //need more data
			readingBuf = buf[readed:required]
			continue
		}
		n = int(readed)
		//one frame readed
		break
	}
	return
}

func (f *FrameReader) String() string {
	return fmt.Sprintf("%p,%v,%p", f, f.Offset, f.Raw)
}

//FrameWriter impl the writer to write data as frame.
//for more info about FrameWriter, see FrameWriter.Write.
type FrameWriter struct {
	Raw    io.Writer //raw writer
	Offset int       //the begin position to save frame length
}

//NewFrameWriter is creator by raw writer.
func NewFrameWriter(raw io.Writer) (writer *FrameWriter) {
	writer = &FrameWriter{
		Raw: raw,
	}
	return
}

//Write data as frame, 4 byte is required to save frame length on buffer p and it's begin position is FrameWriter.Offset.
//so the total size of buffer is 4 byte more than the data size.
func (f *FrameWriter) Write(p []byte) (n int, err error) {
	binary.BigEndian.PutUint32(p, uint32(len(p)))
	if len(p) > 1000 {
		fmt.Println("--->", p)
		panic("errro")
	}
	n, err = f.Raw.Write(p)
	return
}

func (f *FrameWriter) String() string {
	return fmt.Sprintf("%p,%v,%p", f, f.Offset, f.Raw)
}

type NoneCloserReader struct {
	io.Reader
}

func NewNoneCloserReader(reader io.Reader) *NoneCloserReader {
	return &NoneCloserReader{Reader: reader}
}

func (n *NoneCloserReader) Close() (err error) {
	return
}

func (n *NoneCloserReader) String() string {
	return fmt.Sprintf("%v", n.Reader)
}

type NoneCloserWriter struct {
	io.Writer
}

func NewNoneCloserWriter(writer io.Writer) *NoneCloserWriter {
	return &NoneCloserWriter{Writer: writer}
}

func (n *NoneCloserWriter) String() string {
	return fmt.Sprintf("%v", n.Writer)
}

func (n *NoneCloserWriter) Close() (err error) {
	return
}

type NoneCloserReadWriter struct {
	io.Writer
	io.Reader
}

func NewNoneCloserReadWriter(reader io.Reader, writer io.Writer) *NoneCloserReadWriter {
	return &NoneCloserReadWriter{Reader: reader, Writer: writer}
}

func (n *NoneCloserReadWriter) Close() (err error) {
	return
}

func (n *NoneCloserReadWriter) String() string {
	return fmt.Sprintf("%v,%v", n.Reader, n.Writer)
}

type AutoCloseReadWriter struct {
	io.Reader
	io.Writer
}

func NewAutoCloseReadWriter(reader io.Reader, writer io.Writer) *AutoCloseReadWriter {
	return &AutoCloseReadWriter{Reader: reader, Writer: writer}
}

func (a *AutoCloseReadWriter) Close() (err error) {
	if closer, ok := a.Reader.(io.Closer); ok {
		xerr := closer.Close()
		if xerr != nil {
			err = xerr
		}
	}
	if closer, ok := a.Writer.(io.Closer); ok {
		xerr := closer.Close()
		if xerr != nil {
			err = xerr
		}
	}
	return
}

func (a *AutoCloseReadWriter) String() string {
	return fmt.Sprintf("%v,%v", a.Reader, a.Writer)
}
