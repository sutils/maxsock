package main

import (
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/Centny/gwf/log"

	"github.com/Centny/gwf/util"
	"github.com/sutils/maxsock"
)

const RunnerOffset = maxsock.FrameOffset + maxsock.ConsistentOffset

var Cfg = util.NewFcfg3()

func main() {
	var conf = "conf/maxsock.properties"
	if len(os.Args) > 2 {
		conf = os.Args[2]
	}
	Cfg.InitWithUri2(conf, true)
	//
	if os.Args[1] == "-s" {
		bufferSize := Cfg.IntValV("server/bufferSize", 1024)
		readChanSize := Cfg.IntValV("server/readChanSize", 64)
		bindMax := Cfg.IntValV("server/bindMax", 32)
		queueMax := uint16(Cfg.IntValV("server/queueMax", 1024))
		maxsock.ShowLog = Cfg.IntValV("server/showlog", 0)
		runner := maxsock.NewConsistentListenerTransparentRunner(bufferSize, RunnerOffset)
		listener := maxsock.NewConsistentListener(bufferSize, readChanSize, bindMax, queueMax, 0, runner)
		for key := range Cfg.Map {
			if strings.HasPrefix(key, "acl/") {
				runner.ACL[strings.TrimPrefix(key, "acl/")] = strings.Split(Cfg.Val(key), ",")
			} else if strings.HasPrefix(key, "auth/") {
				listener.Acceptor.AuthKey[strings.TrimPrefix(key, "auth/")] = Cfg.Val(key)
			}
		}
		for _, addr := range strings.Split(Cfg.Val("server/listen_tcp"), ",") {
			err := listener.ListenTCP("tcp", addr)
			if err != nil {
				panic(err)
			}
		}
		for _, addr := range strings.Split(Cfg.Val("server/listen_udp"), ",") {
			err := listener.ListenUDP("udp", addr)
			if err != nil {
				panic(err)
			}
		}
	} else {
		bufferSize := Cfg.IntValV("connector/bufferSize", 1024)
		readChanSize := Cfg.IntValV("connector/readChanSize", 64)
		bindMax := Cfg.IntValV("connector/bindMax", 32)
		queueMax := uint16(Cfg.IntValV("connector/queueMax", 1024))
		name := Cfg.Val2("connector/name", "unset")
		auth := Cfg.Val2("connector/auth", "unset")
		maxsock.ShowLog = Cfg.IntValV("connector/showlog", 0)
		connector := maxsock.NewConsistentConnector(name, auth, bufferSize, readChanSize, bindMax, queueMax, 0)
		var back = &maxsock.AuthOption{}
		var conn io.ReadWriteCloser
		for key := range Cfg.Map {
			if !strings.HasPrefix(key, "channel") {
				continue
			}
			uri := Cfg.Val(key)
			parts := strings.Split(uri, "/")
			if len(parts) < 4 {
				log.E("channel uri is not correct:%v", uri)
				continue
			}
			count, err := strconv.Atoi(parts[3])
			if err != nil {
				log.E("channel uri is not correct:%v->%v", uri, err)
				continue
			}
			if parts[0] == "tcp" {
				for i := 0; i < count; i++ {
					back, conn, _, err = connector.DailTCP(parts[0], parts[1], parts[2], back.Session, nil)
					if err != nil {
						log.E("connect channel uri(%v) failw with %v", uri, err)
						continue
					}
				}
			} else {
				for i := 0; i < count; i++ {
					back, conn, _, err = connector.DailUDP(parts[0], parts[1], parts[2], back.Session, nil)
					if err != nil {
						log.E("connect channel uri(%v) failw with %v", uri, err)
						continue
					}
				}
			}

		}
		runner := maxsock.NewConsistentTransparentRunner(name, back.Session, bufferSize, RunnerOffset, conn)
		for key := range Cfg.Map {
			if !strings.HasPrefix(key, "proxy") {
				continue
			}
			uri := Cfg.Val(key)
			parts := strings.Split(uri, "/")
			if len(parts) < 3 {
				log.E("proxy uri is not correct:%v", uri)
				continue
			}
			// if parts[0] == "tcp" {
			listener, err := net.Listen("tcp", parts[1])
			if err != nil {
				log.E("listent proxy uri(%v) fail with %v", uri, err)
				continue
			}
			log.D("listen proxy on tcp(%v)", parts[1])
			go runTCPListener(runner, listener, parts[0], parts[2])
		}
	}
	wait := make(chan int)
	wait <- 1
}

func runTCPListener(runner *maxsock.ConsistentTransparentRunner, listener net.Listener, network, remote string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.E("proxy(%v) accept fail with %v", listener.Addr(), err)
			return
		}
		_, err = runner.Connect(conn, network, remote)
		if err != nil {
			log.E("proxy(%v) connect to %v(%v) fail with %v", listener.Addr(), network, remote, err)
			conn.Close()
		}
	}
}

func runUDPConn(runner *maxsock.ConsistentTransparentRunner, conn *net.UDPConn, network, remote string) {

}
