package client

import (
	"errors"
	"net"

	"github.com/zhexuany/esm-filter/run"
)

var (
	ErrClientAlreadyOpened = errors.New("Client is already Opened")
)

type Client struct {
	bindAddr string
	ln       *net.UDPConn
}

func NewClient(config run.Config) *Client {
	c := &Client{bindAddr: config.BindAddress}
	return c
}

func (c *Client) Open() error {
	serverAddr, err := net.ResolveUDPAddr("udp", c.bindAddr)
	if err != nil {
		return err
	}

	serverConn, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		return err
	}
	c.ln = serverConn

	return nil
}

func (c *Client) Close() error {
	return c.ln.Close()
}

func (c *Client) Read() (string, error) {
	buf := make([]byte, 1024)
	n, _, err := c.ln.ReadFromUDP(buf)
	if err != nil {
		return "", err
	}

	//convert bytes array to string
	str := string(buf[:n])

	return str, nil
}

func mapper(input interface{}, output chan interface{}) {

}

func reducer(input chan interface{}, output chan interface{}) {

}
