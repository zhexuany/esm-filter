package client

import (
	"errors"
	"net"
)

var (
	ErrClientAlreadyOpened = errors.New("Client is already Opened")
)

type Client struct {
	hostname string
	bindAddr string
	ln       *net.UDPConn
}

func NewClient(config *Config) *Client {
	c := &Client{bindAddr: config.BindAddress}
	return c
}

func (c *Client) Open() error {
	ip := c.hostname + c.bindAddr
	serverAddr, err := net.ResolveUDPAddr("udp", ip)
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
	if c.ln != nil {
		return c.ln.Close()
	}
	return nil
}

func (c *Client) Read() ([]byte, error) {
	buf := make([]byte, 1024)
	n, _, err := c.ln.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}

	//convert bytes array to string
	return buf[:n], nil
}
