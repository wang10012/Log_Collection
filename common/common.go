package common

import (
	"fmt"
	"net"
	"strings"
)

// log config struct
type CollectEntry struct {
	// for json
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

func GetOutboundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.80")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddress := conn.LocalAddr().(*net.UDPAddr)
	fmt.Println(localAddress.String())
	ip = strings.Split(localAddress.IP.String(), ":")[0]
	return
}
