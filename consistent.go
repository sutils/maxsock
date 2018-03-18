package maxsock

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/Centny/gwf/log"
	"github.com/Centny/gwf/util"
)

type udpConn struct {
	remote   net.Addr
	conn     *net.UDPConn
	readChan chan []byte
	OnClose  func(u *udpConn)
}

func newUDPConn(conn *net.UDPConn, remote net.Addr, onclose func(u *udpConn)) (udp *udpConn) {
	udp = &udpConn{
		remote:   remote,
		conn:     conn,
		readChan: make(chan []byte, 64),
		OnClose:  onclose,
	}
	return
}

func (u *udpConn) Read(p []byte) (n int, err error) {
	buf := <-u.readChan
	if buf == nil {
		err = fmt.Errorf("connection is closed")
		return
	}
	if len(p) < len(buf) {
		err = fmt.Errorf("read buffer is smaller to data, expect %v, but %v found", len(buf), len(p))
		return
	}
	copy(p, buf)
	n = len(buf)
	return
}

func (u *udpConn) Write(p []byte) (n int, err error) {
	n, err = u.conn.WriteTo(p, u.remote)
	return
}

func (u *udpConn) Close() (err error) {
	close(u.readChan)
	u.OnClose(u)
	return
}

type ConsistentListenerEvent interface {
	OnAccept(c *ConsistentListener, conn io.ReadWriteCloser, option *AuthOption, offset int) (err error)
}

type ConsistentListener struct {
	Acceptor       *BindedAcceptor
	allTCPListener map[*net.TCPListener]int
	allUDPListener map[*net.UDPConn]int
	allUDPConn     map[string]*udpConn
	allUDPLck      sync.RWMutex
	Event          ConsistentListenerEvent
	BufferSize     int //the buffer size of read runner.
	ReadChanSize   int
	QueueMax       uint16 //the max of queued data
	Offset         int

	running    bool
	acceptChan chan io.ReadWriteCloser
}

func NewConsistentListener(bufferSize, readChanSize, bindMax int, queueMax uint16, offset int, event ConsistentListenerEvent) (listener *ConsistentListener) {
	listener = &ConsistentListener{
		BufferSize:     bufferSize,
		ReadChanSize:   readChanSize,
		QueueMax:       queueMax,
		acceptChan:     make(chan io.ReadWriteCloser, 8),
		running:        true,
		allTCPListener: map[*net.TCPListener]int{},
		allUDPListener: map[*net.UDPConn]int{},
		allUDPConn:     map[string]*udpConn{},
		allUDPLck:      sync.RWMutex{},
		Event:          event,
		Offset:         offset,
	}
	listener.Acceptor = NewBindedAcceptor(bufferSize, readChanSize, bindMax, listener.Offset+FrameOffset)
	listener.Acceptor.Event = listener
	go listener.runConsistentAccept()
	return
}

func (c *ConsistentListener) ListenTCP(network, addr string) (err error) {
	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return
	}
	listener, err := net.ListenTCP(network, tcpAddr)
	if err == nil {
		log.D("ConsistentListener(%v) listen tcp on %v", c, addr)
		c.allTCPListener[listener] = 1
		go c.runAcceptTCP(listener)
	}
	return
}

func (c *ConsistentListener) runAcceptTCP(listener *net.TCPListener) (err error) {
	var rawCon net.Conn
	for c.running {
		rawCon, err = listener.Accept()
		if err != nil {
			return
		}
		log.D("ConsistentListener(%v) accept tcp connection from %v", c, rawCon.RemoteAddr())
		frameReader := NewFrameReader(rawCon, c.Offset)
		frameWriter := NewFrameWriter(rawCon, c.Offset)
		frameCon := NewAutoCloseReadWriter(frameReader, frameWriter)
		c.acceptChan <- frameCon
	}
	log.D("ConsistentListener(%v) the tcp accept runner is stopped", c)
	return
}

func (c *ConsistentListener) ListenUDP(network, addr string) (err error) {
	udpAddr, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return
	}
	listener, err := net.ListenUDP(network, udpAddr)
	if err == nil {
		c.allUDPListener[listener] = 1
		go c.runAcceptUDP(listener)
	}
	return
}

func (c *ConsistentListener) runAcceptUDP(listener *net.UDPConn) (err error) {
	for c.running {
		buf := make([]byte, c.BufferSize)
		readed, remote, rerr := listener.ReadFrom(buf)
		if rerr != nil {
			err = rerr
			return
		}
		address := remote.String()
		conn := c.allUDPConn[address]
		if conn == nil {
			conn = newUDPConn(listener, remote, c.onUDPClose)
			c.allUDPConn[address] = conn
			log.D("ConsistentListener(%v) accept udp connection from %v", c, address)
			frameReader := conn
			frameWriter := NewFrameWriter(conn, c.Offset)
			frameCon := NewAutoCloseReadWriter(frameReader, frameWriter)
			c.acceptChan <- frameCon
		}
		conn.readChan <- buf[:readed]
	}
	log.D("ConsistentListener(%v) the udp accept runner is stopped", c)
	return
}

func (c *ConsistentListener) onUDPClose(u *udpConn) {
	c.allUDPLck.Lock()
	delete(c.allUDPConn, u.remote.String())
	c.allUDPLck.Unlock()
}

func (c *ConsistentListener) runConsistentAccept() (err error) {
	for c.running {
		rawCon := <-c.acceptChan
		if rawCon == nil {
			break
		}
		option, _, err := c.Acceptor.Accept(rawCon)
		if err != nil {
			log.D("ConsistentListener(%v) accept to binded fail with %v", c, err)
			rawCon.Close()
		} else {
			log.D("ConsistentListener(%v) accept to binded(%v) by name(%v) success", c, option.Session, option.Name)
		}
	}
	log.D("ConsistentListener(%v) the consistent accept runner is stopped", c)
	return
}

func (c *ConsistentListener) OnAccept(b *BindedAcceptor, channel io.ReadWriteCloser, option *AuthOption) (err error) {
	conn := NewConsistentReadWriter(channel, c.BufferSize, c.ReadChanSize, c.QueueMax, c.Offset+FrameOffset)
	err = c.Event.OnAccept(c, conn, option, c.Offset+FrameOffset+ConsistentOffset)
	return
}

func (c *ConsistentListener) CloseSession(sid uint32) (err error) {
	err = c.Acceptor.CloseSession(sid)
	return
}

func (c *ConsistentListener) Close() (err error) {
	for listener := range c.allTCPListener {
		cerr := listener.Close()
		if cerr != nil {
			err = cerr
		}
	}
	for listener := range c.allUDPListener {
		cerr := listener.Close()
		if cerr != nil {
			err = cerr
		}
	}
	for _, conn := range c.allUDPConn {
		cerr := conn.Close()
		if cerr != nil {
			err = cerr
		}
	}
	cerr := c.Acceptor.Close()
	if cerr != nil {
		err = cerr
	}
	close(c.acceptChan)
	return
}

type ConsistentConnector struct {
	Connector    *BindedConnector
	BufferSize   int //the buffer size of read runner.
	ReadChanSize int
	QueueMax     uint16 //the max of queued data
	Offset       int
}

func NewConsistentConnector(name, authKey string, bufferSize, readChanSize, bindMax int, queueMax uint16, offset int) (conn *ConsistentConnector) {
	conn = &ConsistentConnector{
		BufferSize:   bufferSize,
		ReadChanSize: readChanSize,
		QueueMax:     queueMax,
		Offset:       offset,
	}
	conn.Connector = NewBindedConnector(name, authKey, bufferSize, readChanSize, bindMax, conn.Offset+FrameOffset)
	return
}

func (c *ConsistentConnector) DailTCP(network, local, remote string, session uint32, options util.Map) (back *AuthOption, conn io.ReadWriteCloser, offset int, err error) {
	localAddr, err := net.ResolveTCPAddr(network, local)
	if err != nil {
		return
	}
	remoteAddr, err := net.ResolveTCPAddr(network, remote)
	if err != nil {
		return
	}
	tcpConn, err := net.DialTCP(network, localAddr, remoteAddr)
	if err != nil {
		return
	}
	frameReader := NewFrameReader(tcpConn, c.Offset)
	frameWriter := NewFrameWriter(tcpConn, c.Offset)
	frameCon := NewAutoCloseReadWriter(frameReader, frameWriter)
	back, binded, err := c.Connector.Connect(session, options, frameCon)
	if err == nil {
		conn = NewConsistentReadWriter(binded, c.BufferSize, c.ReadChanSize, c.QueueMax, c.Offset+FrameOffset)
	}
	offset = c.Offset + FrameOffset + ConsistentOffset
	log.D("ConsistentConnector(%v) connect to %v by local(%v),session(%v)", c, remote, local, back.Session)
	return
}

func (c *ConsistentConnector) DailUDP(network, local, remote string, session uint32, options util.Map) (back *AuthOption, conn io.ReadWriteCloser, offset int, err error) {
	localAddr, err := net.ResolveUDPAddr(network, local)
	if err != nil {
		return
	}
	remoteAddr, err := net.ResolveUDPAddr(network, remote)
	if err != nil {
		return
	}
	udpConn, err := net.DialUDP(network, localAddr, remoteAddr)
	if err != nil {
		return
	}
	frameReader := udpConn
	frameWriter := NewFrameWriter(udpConn, c.Offset)
	frameCon := NewAutoCloseReadWriter(frameReader, frameWriter)
	back, binded, err := c.Connector.Connect(session, options, frameCon)
	if err == nil {
		conn = NewConsistentReadWriter(binded, c.BufferSize, c.ReadChanSize, c.QueueMax, c.Offset+FrameOffset)
	}
	offset = c.Offset + FrameOffset + ConsistentOffset
	return
}

type ConsistentReadWriter struct {
	Userinfo interface{}
	*ConsistentReader
	*ConsistentWriter
}

func NewConsistentReadWriter(channel io.ReadWriter, bufferSize, readChanSize int, queueMax uint16, offset int) (conn *ConsistentReadWriter) {
	writer := NewConsistentWriter(channel, queueMax, offset)
	reader := NewConsistentReader(writer, channel, bufferSize, readChanSize, queueMax, offset)
	conn = &ConsistentReadWriter{
		ConsistentReader: reader,
		ConsistentWriter: writer,
	}
	return
}

func (c *ConsistentReadWriter) Close() (err error) {
	if closer, ok := c.ConsistentReader.Raw.(io.Closer); ok {
		xerr := closer.Close()
		if xerr != nil {
			err = xerr
		}
	}
	if closer, ok := c.ConsistentWriter.Raw.(io.Closer); ok {
		xerr := closer.Close()
		if xerr != nil {
			err = xerr
		}
	}
	return
}

//ConsistentReaderEvent is the interface of ConsistentReader
type ConsistentReaderEvent interface {
	OnSendHeartbeat(c *ConsistentReader, current uint16, missing []uint16) (err error)
	OnRecvHeartbeat(c *ConsistentReader, current uint16, missing []uint16) (err error)
}

const ConsistentOffset = 4

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
func NewConsistentReader(event ConsistentReaderEvent, raw io.Reader, bufferSize, readChanSize int, queueMax uint16, offset int) (reader *ConsistentReader) {
	reader = &ConsistentReader{
		Raw:        raw,
		Offset:     offset,
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
func NewConsistentWriter(raw io.Writer, queueMax uint16, offset int) (writer *ConsistentWriter) {
	writer = &ConsistentWriter{
		Raw:        raw,
		Offset:     offset,
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
	if len(p) < c.Offset+4 {
		err = fmt.Errorf("buffer must be having at least %v byte offset", c.Offset+4)
		return
	}
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
