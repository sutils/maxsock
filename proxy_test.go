package maxsock

import (
	"fmt"
	"testing"
)

func TestProxy(t *testing.T) {
	xConA, xConB := channelPipe()
	fmt.Println("-->", xConA, xConB)
	server := NewProxyServer(8, 1024)
	server.AuthKey["x1"] = "abc"
	go func() {
		_, _, err := server.ProcChannel(xConB)
		if err != nil {
			panic(err)
		}
	}()
	client := NewProxyClient("x1", "abc", 8, 1024)
	back, conn, err := client.ProcChannel(0, xConA)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(back, conn)

}
