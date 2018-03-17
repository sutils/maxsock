package maxsock

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const ConnSignalAuth = 10

type AuthOption struct {
	Name    string `json:"name"`
	Session uint32 `json:"session"`
	Error   error  `json:"error"`
}

type ProxServer struct {
	AuthKey                 map[string]string
	authKeyLck              sync.RWMutex
	sequence                uint32
	binded                  map[uint32]*Conn
	bindedLck               sync.RWMutex
	BufferLimit, BufferSize int
}

func NewProxyServer(bufferLimit, bufferSize int) (server *ProxServer) {
	server = &ProxServer{
		AuthKey:     map[string]string{},
		authKeyLck:  sync.RWMutex{},
		binded:      map[uint32]*Conn{},
		bindedLck:   sync.RWMutex{},
		BufferLimit: bufferLimit,
		BufferSize:  bufferSize,
	}
	return
}

func (p *ProxServer) ProcChannel(channel io.ReadWriteCloser) (option *AuthOption, conn *Conn, err error) {
	header := make([]byte, 5)
	err = FullRead(channel, header)
	if err != nil {
		return
	}
	fmt.Println("readed->")
	bufLen := binary.BigEndian.Uint32(header[1:])
	buf := make([]byte, bufLen-3)
	err = FullRead(channel, buf)
	if err != nil {
		return
	}
	fmt.Println("readed->")
	option = &AuthOption{}
	var back = &AuthOption{}
	err = json.Unmarshal(buf[20:], option)
	if err != nil {
		err = fmt.Errorf("pass auth option fail with %v", err)
		back.Error = err
		_, err = WriteJSON(channel, ConnSignalAuth, back)
		return
	}
	p.authKeyLck.Lock()
	authKey := p.AuthKey[option.Name]
	p.authKeyLck.Unlock()
	//
	authBuf := make([]byte, int(bufLen)-23+len(p.AuthKey))
	copy(authBuf, buf[20:])
	copy(authBuf[bufLen-23:], []byte(authKey))
	shaHash := sha1.New()
	shaHash.Write(authBuf)
	if bytes.Compare(shaHash.Sum(nil), buf[0:20]) != 0 {
		err = fmt.Errorf("auth fail")
		back.Error = err
		_, err = WriteJSON(channel, ConnSignalAuth, back)
		return
	}
	//
	//do acl

	//
	if option.Session > 0 {
		p.bindedLck.Lock()
		conn = p.binded[option.Session]
		p.bindedLck.Unlock()
		if conn == nil {
			err = fmt.Errorf("session not found by session(%v)", option.Session)
			back.Error = err
			_, err = WriteJSON(channel, ConnSignalAuth, back)
			return
		}
		back.Error = nil
		back.Session = option.Session
		_, err = WriteJSON(channel, ConnSignalAuth, back)
		if err == nil {
			conn.Bind(channel)
		}
		return
	}
	back.Error = nil
	back.Session = atomic.AddUint32(&p.sequence, 1)
	_, err = WriteJSON(channel, ConnSignalAuth, back)
	if err == nil {
		conn = NewConn(back.Session, p.BufferLimit, p.BufferSize)
		conn.Userinfo = option
		p.bindedLck.Lock()
		p.binded[back.Session] = conn
		p.bindedLck.Unlock()
		conn.Bind(channel)
	}
	return
}

type ProxyClient struct {
	Name                    string
	AuthKey                 string
	binded                  map[uint32]*Conn
	bindedLck               sync.RWMutex
	BufferLimit, BufferSize int
}

func NewProxyClient(name, authKey string, bufferLimit, bufferSize int) (client *ProxyClient) {
	client = &ProxyClient{
		Name:        name,
		AuthKey:     authKey,
		binded:      map[uint32]*Conn{},
		bindedLck:   sync.RWMutex{},
		BufferLimit: bufferLimit,
		BufferSize:  bufferSize,
	}
	return
}

func (p *ProxyClient) ProcChannel(session uint32, channel io.ReadWriteCloser) (back *AuthOption, conn *Conn, err error) {
	data, _ := json.Marshal(&AuthOption{
		Name:    p.Name,
		Session: session,
	})
	authBuf := make([]byte, len(data)+len(p.AuthKey))
	copy(authBuf, data)
	copy(authBuf[len(data):], []byte(p.AuthKey))
	shaHash := sha1.New()
	shaHash.Write(authBuf)
	authData := make([]byte, len(data)+20)
	hash := sha1.Sum(nil)
	copy(authData, hash[:])
	copy(authData[20:], data)
	_, err = WriteFrame(channel, ConnSignalAuth, authData)
	if err != nil {
		return
	}
	fmt.Println("--->")
	//
	header := make([]byte, 5)
	err = FullRead(channel, header)
	if err != nil {
		return
	}
	bufLen := binary.BigEndian.Uint32(header[1:])
	buf := make([]byte, bufLen-3)
	err = FullRead(channel, buf)
	if err != nil {
		return
	}
	back = &AuthOption{}
	err = json.Unmarshal(buf, back)
	if back.Error != nil {
		err = back.Error
		return
	}
	p.bindedLck.Lock()
	conn = p.binded[back.Session]
	if conn == nil {
		conn = NewConn(back.Session, p.BufferLimit, p.BufferSize)
		conn.Userinfo = back
	}
	p.bindedLck.Unlock()
	conn.Bind(channel)
	return
}

func FullRead(r io.ReadCloser, p []byte) error {
	l := len(p)
	all := 0
	buf := p
	for {
		oneReaded, err := r.Read(buf)
		fmt.Println(r, "---->rree", len(buf))
		if err != nil {
			return err
		}
		all += oneReaded
		if all < l {
			buf = p[all:]
			continue
		} else {
			break
		}
	}
	return nil
}

func CopyAndClose(dst, src io.ReadWriteCloser) {
	io.Copy(dst, src)
	dst.Close()
	src.Close()
}
