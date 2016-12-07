package client

import (
	"net"
	"testing"
)

func TestClient_Read(t *testing.T) {
	testPort := ":9090"
	testMsg := "test"

	go func() {
		serverAddr, err := net.ResolveUDPAddr("udp", testPort)
		conn, err := net.DialUDP("udp", nil, serverAddr)
		if err != nil {
			t.Errorf("failed to create udp connection", err)
		}

		for {
			_, err = conn.Write([]byte(testMsg))
			if err != nil {
				t.Errorf("failed to write message via udp", err)
			}

		}
	}()
	clientAddr, err := net.ResolveUDPAddr("udp", testPort)
	if err != nil {
		t.Errorf("failed to create server address", err)
	}

	c := &Client{}
	clientLn, err := net.ListenUDP("udp", clientAddr)
	if err != nil {
		t.Errorf("failed to create listener", err)
	}
	c.ln = clientLn
	str, err := c.Read()
	if err != nil {
		t.Errorf("failed to read", err)
	}
	if str != testMsg {
		t.Errorf("Expected the message %s wth length %d but found %s with length %d", testMsg, len(testMsg), str, len(str))
	}
}
