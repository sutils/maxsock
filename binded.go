package maxsock

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/Centny/gwf/log"
	"github.com/Centny/gwf/util"
)

type AuthOption struct {
	Name    string   `json:"name"`
	Session uint32   `json:"session"`
	Error   string   `json:"error"`
	Options util.Map `json:"options"`
}

func WriteJSON(w io.Writer, offset int, v interface{}) (n int, err error) {
	data, err := json.Marshal(v)
	if err == nil {
		n = offset + len(data)
		buf := make([]byte, n)
		copy(buf[offset:], data)
		n, err = w.Write(buf)
	}
	return
}

func hashSha1(authKey string, data []byte) (hash []byte) {
	authKeyByte := []byte(authKey)
	hashBuf := make([]byte, len(data)+len(authKeyByte))
	copy(hashBuf, authKeyByte)
	copy(hashBuf[len(authKeyByte):], data)
	shaHash := sha1.New()
	shaHash.Write(hashBuf)
	hash = shaHash.Sum(nil)
	return
}

type BindedAcceptorEvent interface {
	OnAccept(b *BindedAcceptor, channel io.ReadWriteCloser, option *AuthOption) (err error)
}

type BindedAcceptor struct {
	BufferSize   int
	Offset       int
	ReadChanSize int
	BindMax      int
	AuthKey      map[string]string
	Event        BindedAcceptorEvent

	allBinded  map[uint32]*BindedReadWriter
	bindedLck  sync.RWMutex
	authKeyLck sync.RWMutex
	sequence   uint32
}

func NewBindedAcceptor(bufferSize, readChanSize, bindMax, offset int) (acceptor *BindedAcceptor) {
	acceptor = &BindedAcceptor{
		BufferSize:   bufferSize,
		Offset:       offset,
		ReadChanSize: readChanSize,
		BindMax:      bindMax,
		AuthKey:      map[string]string{},
		allBinded:    map[uint32]*BindedReadWriter{},
		bindedLck:    sync.RWMutex{},
		authKeyLck:   sync.RWMutex{},
	}
	return
}

func (b *BindedAcceptor) Accept(channel io.ReadWriteCloser) (option *AuthOption, binded *BindedReadWriter, err error) {
	buf := make([]byte, b.BufferSize)
	readed, err := channel.Read(buf)
	if err != nil {
		return
	}
	if readed < b.Offset+20 {
		err = fmt.Errorf("receive bad data")
		return
	}
	buf = buf[:readed]
	option = &AuthOption{Options: util.Map{}}
	back := &AuthOption{Options: util.Map{}}
	err = json.Unmarshal(buf[b.Offset+20:], option)
	if err != nil {
		err = fmt.Errorf("pass auth option fail with %v", err)
		back.Error = err.Error()
		log.W("BindedAcceptor(%v) %v", b, err)
		_, err = WriteJSON(channel, b.Offset, back)
		return
	}
	//
	b.authKeyLck.Lock()
	authKey := b.AuthKey[option.Name]
	b.authKeyLck.Unlock()
	//
	hash := hashSha1(authKey, buf[b.Offset+20:])
	if bytes.Compare(hash, buf[b.Offset:b.Offset+20]) != 0 {
		err = fmt.Errorf("auth fail")
		back.Error = err.Error()
		log.W("BindedAcceptor(%v) auth fail with hash not match", b)
		_, err = WriteJSON(channel, b.Offset, back)
		return
	}
	//
	//
	if option.Session > 0 {
		b.bindedLck.Lock()
		binded = b.allBinded[option.Session]
		b.bindedLck.Unlock()
		if binded == nil {
			err = fmt.Errorf("session not found by session(%v)", option.Session)
			back.Error = err.Error()
			log.W("BindedAcceptor(%v) %v", b, err)
			_, err = WriteJSON(channel, b.Offset, back)
			return
		}
		back.Error = ""
		back.Session = option.Session
		_, err = WriteJSON(channel, b.Offset, back)
		if err != nil {
			return
		}
		err = binded.Bind(channel, channel)
		if ShowLog > 0 {
			log.D("BindedAcceptor(%v) bind conn by connected session(%v)", b, back.Session)
		}
		return
	}
	back.Error = ""
	back.Session = atomic.AddUint32(&b.sequence, 1)
	option.Session = back.Session
	_, err = WriteJSON(channel, b.Offset, back)
	if err != nil {
		return
	}
	binded = NewBindedReadWriter(b.BufferSize, b.ReadChanSize, b.BindMax)
	binded.Userinfo = option
	binded.Bind(channel, channel)
	if b.Event != nil {
		err = b.Event.OnAccept(b, binded, option)
		if err != nil {
			back.Error = err.Error()
			log.W("BindedAcceptor(%v) event on accept fail with %v", b, err)
			_, err = WriteJSON(channel, b.Offset, back)
			return
		}
	}
	b.bindedLck.Lock()
	b.allBinded[back.Session] = binded
	b.bindedLck.Unlock()
	if ShowLog > 0 {
		log.D("BindedAcceptor(%v) bind conn by new session(%v)", b, back.Session)
	}
	return
}

func (b *BindedAcceptor) CloseSession(sid uint32) (err error) {
	b.bindedLck.Lock()
	binded := b.allBinded[sid]
	if binded == nil {
		err = fmt.Errorf("not exist")
	} else {
		err = binded.Close()
	}
	b.bindedLck.Unlock()
	return
}

func (b *BindedAcceptor) Close() (err error) {
	b.bindedLck.Lock()
	for _, binded := range b.allBinded {
		cerr := binded.Close()
		if cerr != nil {
			err = cerr
		}
	}
	b.bindedLck.Unlock()
	return
}

func (b *BindedAcceptor) String() string {
	return fmt.Sprintf("%p,%v", b, b.Offset)
}

type BindedConnector struct {
	Name         string
	AuthKey      string
	BufferSize   int
	Offset       int
	ReadChanSize int
	BindMax      int
	allBinded    map[uint32]*BindedReadWriter
	bindedLck    sync.RWMutex
}

func NewBindedConnector(name, authKey string, bufferSize, readChanSize, bindMax, offset int) (connector *BindedConnector) {
	connector = &BindedConnector{
		Name:         name,
		AuthKey:      authKey,
		BufferSize:   bufferSize,
		Offset:       offset,
		ReadChanSize: readChanSize,
		BindMax:      bindMax,
		allBinded:    map[uint32]*BindedReadWriter{},
		bindedLck:    sync.RWMutex{},
	}
	return
}

func (b *BindedConnector) Connect(session uint32, options util.Map, channel io.ReadWriteCloser) (back *AuthOption, binded *BindedReadWriter, err error) {
	option := &AuthOption{
		Name:    b.Name,
		Session: session,
		Options: options,
	}
	data, err := json.Marshal(option)
	if err != nil {
		return
	}
	hash := hashSha1(b.AuthKey, data)
	authData := make([]byte, b.Offset+len(data)+20)
	copy(authData[b.Offset:], hash)
	copy(authData[b.Offset+20:], data)
	_, err = channel.Write(authData)
	if err != nil {
		return
	}
	//
	buf := make([]byte, b.BufferSize)
	readed, err := channel.Read(buf)
	if err != nil {
		return
	}
	buf = buf[:readed]
	back = &AuthOption{Options: util.Map{}}
	err = json.Unmarshal(buf[b.Offset:], back)
	if err != nil {
		err = fmt.Errorf("pass auth return fail with %v", err)
		return
	}
	//
	b.bindedLck.Lock()
	binded = b.allBinded[back.Session]
	if binded == nil {
		binded = NewBindedReadWriter(b.BufferSize, b.ReadChanSize, b.BindMax)
		b.allBinded[back.Session] = binded
		binded.Userinfo = option
		if ShowLog > 0 {
			log.D("BindedConnector(%v) bind conn by new session(%v)", b, back.Session)
		}
	} else {
		if ShowLog > 0 {
			log.D("BindedConnector(%v) bind conn by connected session(%v)", b, back.Session)
		}
	}
	b.bindedLck.Unlock()
	err = binded.Bind(channel, channel)
	return
}

func (b *BindedConnector) Close() (err error) {
	b.bindedLck.Lock()
	for _, binded := range b.allBinded {
		cerr := binded.Close()
		if cerr != nil {
			err = cerr
		}
	}
	b.bindedLck.Unlock()
	return
}

func (b *BindedConnector) String() string {
	return fmt.Sprintf("%p,%v", b, b.Offset)
}

//BindedReaderEvent is the interface of the BindedReader event.
type BindedReaderEvent interface {
	//call it on the runner of per raw reader is stopped.
	OnRawDone(b *BindedReader, raw io.Reader, err error)
}

//BindedReader impl the reader which can bind multi raw reader to one and balanced.
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

//NewBindedReader is the creator of BindedReader by read buffer size, read chan cache size, and pool size.
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

//Bind a raw reader to pool
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
	if ShowLog > 0 {
		log.D("BindedReader(%v) raw reader is starting", b)
	}
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
	if ShowLog > 0 {
		log.D("BindedReader(%v) raw reader is done with error:%v", b, err)
	}
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

//Close all binded raw reader.
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

//BindedWriter impl the writer whihc can bind mulit raw writer to one and balanced.
type BindedWriter struct {
	Max        int
	binded     map[io.WriteCloser]int
	bindedLck  chan int
	bindedChan chan io.Writer
	running    bool
}

//NewBindedWriter is the creator of BindedWriter by max size of pool
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

//Bind one raw writer to pool
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

//Close all binded raw writer
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

type BindedReadWriter struct {
	*BindedReader
	*BindedWriter
	Userinfo interface{}
}

func NewBindedReadWriter(bufferSize, readChanSize, max int) (binded *BindedReadWriter) {
	return &BindedReadWriter{
		BindedReader: NewBindedReader(bufferSize, readChanSize, max),
		BindedWriter: NewBindedWriter(max),
	}
}

func (b *BindedReadWriter) Bind(reader io.ReadCloser, writer io.WriteCloser) (err error) {
	err = b.BindedReader.Bind(reader)
	if err == nil {
		err = b.BindedWriter.Bind(writer)
	}
	return
}

func (b *BindedReadWriter) Close() (err error) {
	cerr := b.BindedReader.Close()
	if cerr != nil {
		err = cerr
	}
	cerr = b.BindedWriter.Close()
	if cerr != nil {
		err = cerr
	}
	return
}
