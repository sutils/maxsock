package maxsock

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"

	"github.com/Centny/gwf/log"
	"github.com/Centny/gwf/util"
)

type ConsistentListenerTransparentRunner struct {
	ACL            map[string][]string
	BufferSize     int
	Offset         int
	allRunner      map[uint32]*ConsistentTransparentRunner
	allSessionName map[uint32]string
	allRunnerLck   sync.RWMutex
	running        bool
}

func NewConsistentListenerTransparentRunner(bufferSize, offset int) (runner *ConsistentListenerTransparentRunner) {
	runner = &ConsistentListenerTransparentRunner{
		ACL:            map[string][]string{},
		BufferSize:     bufferSize,
		Offset:         offset,
		allRunner:      map[uint32]*ConsistentTransparentRunner{},
		allSessionName: map[uint32]string{},
		allRunnerLck:   sync.RWMutex{},
		running:        true,
	}
	return
}

func (c *ConsistentListenerTransparentRunner) StartTimeout(timeout int64) {
	go c.runTimeout(timeout)
}

func (c *ConsistentListenerTransparentRunner) runTimeout(timeout int64) {
	for c.running {
		c.allRunnerLck.Lock()
		now := util.Now()
		for sid, runner := range c.allRunner {
			if now-runner.Last < timeout {
				continue
			}
			runner.Close()
			delete(c.allRunner, sid)
		}
		c.allRunnerLck.Unlock()
	}
}

func (c *ConsistentListenerTransparentRunner) OnAccept(l *ConsistentListener, conn io.ReadWriteCloser, option *AuthOption, offset int) (err error) {
	c.allRunnerLck.Lock()
	if c.running {
		runner := NewConsistentTransparentRunner(option.Name, option.Session, c.BufferSize, c.Offset, conn)
		runner.ACL, runner.Userinfo = c.ACL[option.Name], l
		c.allRunner[option.Session] = runner
		c.allSessionName[option.Session] = option.Name
	} else {
		err = fmt.Errorf("runner is closed")
	}
	c.allRunnerLck.Unlock()
	return
}

func (c *ConsistentListenerTransparentRunner) OnRunnerClose(runner *ConsistentTransparentRunner) {
	runner.Userinfo.(*ConsistentListener).CloseSession(runner.Session)
}

func (c *ConsistentListenerTransparentRunner) NamedRunner(name string) (runner *ConsistentTransparentRunner) {
	c.allRunnerLck.RLock()
	defer c.allRunnerLck.RUnlock()
	for sid, n := range c.allSessionName {
		if n == name {
			runner = c.allRunner[sid]
			break
		}
	}
	return
}

func (c *ConsistentListenerTransparentRunner) Connect(name string, raw io.ReadWriteCloser, network, remote string) (sid uint16, err error) {
	runner := c.NamedRunner(name)
	if runner != nil {
		err = fmt.Errorf("runner is not found")
		return
	}
	sid, err = runner.Connect(raw, network, remote)
	return
}

func (c *ConsistentListenerTransparentRunner) CloseSid(name string, sid uint16) (err error) {
	runner := c.NamedRunner(name)
	if runner != nil {
		err = fmt.Errorf("runner is not found")
		return
	}
	err = runner.CloseSid(sid)
	return
}

func (c *ConsistentListenerTransparentRunner) Close() (err error) {
	c.allRunnerLck.Lock()
	c.running = false
	for sid, runner := range c.allRunner {
		runner.Close()
		delete(c.allRunner, sid)
	}
	c.allRunnerLck.Unlock()
	return
}

type ConsistentTransparentRunnerEvent interface {
	OnRunnerClose(runner *ConsistentTransparentRunner)
}

type ConsistentTransparentRunner struct {
	Name       string
	Session    uint32
	BufferSize int
	ACL        []string
	Channel    io.ReadWriteCloser
	aclLck     sync.RWMutex
	Offset     int
	running    bool
	sequence   uint16
	allConn    map[uint16]io.ReadWriteCloser
	allConnLck sync.RWMutex
	connected  chan util.Map
	Last       int64
	Event      ConsistentTransparentRunnerEvent
	Userinfo   interface{}
}

func NewConsistentTransparentRunner(name string, session uint32, bufferSize, offset int, channel io.ReadWriteCloser) (transpart *ConsistentTransparentRunner) {
	transpart = &ConsistentTransparentRunner{
		Name:       name,
		Session:    session,
		BufferSize: bufferSize,
		Offset:     offset,
		Channel:    channel,
		aclLck:     sync.RWMutex{},
		running:    true,
		allConn:    map[uint16]io.ReadWriteCloser{},
		allConnLck: sync.RWMutex{},
		connected:  make(chan util.Map, 1),
		Last:       util.Now(),
	}
	go transpart.runConsistentCon(channel)
	return
}

func (c *ConsistentTransparentRunner) doConnect(args util.Map) (sid uint16, conn net.Conn, err error) {
	var network, remote = "tcp", ""
	err = args.ValidF(`
		network,O|S,L:0;
		remote,R|S,L:0;
		`, &network, &remote)
	if err != nil {
		return
	}
	var matched string
	for _, acl := range c.ACL {
		m, cerr := regexp.Compile(acl)
		if cerr != nil {
			err = cerr
			log.E("ConsistentTransparent compile acl regex(%v) on name(%v) fail with %v", acl, c.Name, err)
			return
		}
		if m.MatchString(remote) {
			matched = acl
			break
		}
	}
	if len(matched) < 1 {
		err = fmt.Errorf("%v not access for %v", c.Name, remote)
		return
	}
	log.D("ConsistentTransparent do acl passed on name(%v) by regex(%v)", c.Name, matched)
	rawCon, err := net.Dial(network, remote)
	if err != nil {
		return
	}
	c.allConnLck.Lock()
	c.sequence++
	sid = c.sequence
	c.allConn[sid] = rawCon
	c.allConnLck.Unlock()
	go c.runRawConn(sid, rawCon)
	log.D("ConsistentTransparent dail to %v(%v) success with sid(%v)", network, remote, sid)
	return
}

func (c *ConsistentTransparentRunner) runRawConn(sid uint16, conn io.ReadWriteCloser) {
	log.D("ConsistentTransparentRunner raw runner on %v for sid(%v) is starting", c.Name, sid)
	buf := make([]byte, c.BufferSize)
	readBuf := buf[c.Offset+3:]
	buf[c.Offset] = 0
	binary.BigEndian.PutUint16(buf[c.Offset+1:], sid)
	for c.running {
		readed, err := conn.Read(readBuf)
		if err != nil {
			break
		}
		_, err = c.Channel.Write(buf[:readed+c.Offset+3])
		if err != nil {
			break
		}
	}
	c.CloseSid(sid)
	log.D("ConsistentTransparentRunner raw runner on %v for sid(%v) is stopped", c.Name, sid)
}

func (c *ConsistentTransparentRunner) runConsistentCon(conn io.ReadWriteCloser) {
	log.D("ConsistentTransparentRunner consistent runner on %v is starting", c.Name)
	buf := make([]byte, c.BufferSize)
	for c.running {
		readed, err := conn.Read(buf)
		if err != nil {
			break
		}
		c.Last = util.Now()
		realBuf := buf[c.Offset:readed]
		sid := binary.BigEndian.Uint16(realBuf[1:])
		switch realBuf[0] {
		case 0: //for data
			c.allConnLck.Lock()
			rawCon := c.allConn[sid]
			c.allConnLck.Unlock()
			if rawCon == nil { //send disconnect
				buf[c.Offset] = 12
				conn.Write(buf[:c.Offset+3])
			} else {
				rawCon.Write(realBuf[3:])
			}
		case 10: //do connect
			var args, back = util.Map{}, util.Map{}
			err := json.Unmarshal(realBuf[3:], &args)
			if err == nil {
				sid, _, err = c.doConnect(args)
				back["sid"] = sid
			}
			if err != nil {
				back["error"] = err.Error()
			}
			back["key"] = args["key"]
			//
			bys, _ := json.Marshal(back)
			buf[c.Offset] = 11
			binary.BigEndian.PutUint16(buf[c.Offset+1:], sid)
			copy(buf[c.Offset+3:], bys)
			conn.Write(buf[:c.Offset+3+len(bys)])
		case 11: //do connect return
			var args = util.Map{}
			err := json.Unmarshal(realBuf[3:], &args)
			if err != nil {
				args["error"] = err.Error()
			}
			c.connected <- args
		case 12: //do disconnect
			c.allConnLck.Lock()
			conn := c.allConn[sid]
			if conn != nil {
				conn.Close()
				delete(c.allConn, sid)
			}
			c.allConnLck.Unlock()
		}
	}
	if c.Event != nil {
		c.Event.OnRunnerClose(c)
	}
	log.D("ConsistentTransparentRunner consistent runner on %v is stopped", c.Name)
}

func (c *ConsistentTransparentRunner) Connect(raw io.ReadWriteCloser, network, remote string) (sid uint16, err error) {
	buf := make([]byte, c.BufferSize)
	buf[c.Offset] = 10
	bys, _ := json.Marshal(util.Map{
		"network": network,
		"remote":  remote,
	})
	copy(buf[c.Offset+3:], bys)
	_, err = c.Channel.Write(buf[:c.Offset+3+len(bys)])
	if err != nil {
		return
	}
	back := <-c.connected
	emsg := back.StrVal("error")
	if len(emsg) > 0 {
		err = fmt.Errorf("%v", emsg)
		return
	}
	sid = uint16(back.IntVal("sid"))
	c.allConnLck.Lock()
	c.allConn[sid] = raw
	c.allConnLck.Unlock()
	go c.runRawConn(sid, raw)
	log.D("ConsistentTransparentRunner connect to %v(%v,%v) success with sid(%v)", c.Name, network, remote, sid)
	return
}

func (c *ConsistentTransparentRunner) CloseSid(sid uint16) (err error) {
	c.allConnLck.Lock()
	delete(c.allConn, sid)
	c.allConnLck.Unlock()
	buf := make([]byte, c.Offset+3)
	buf[c.Offset] = 12
	binary.BigEndian.PutUint16(buf[c.Offset+1:], sid)
	_, err = c.Channel.Write(buf)
	return
}

func (c *ConsistentTransparentRunner) Close() (err error) {
	err = c.Channel.Close()
	return
}
