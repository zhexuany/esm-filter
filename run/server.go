package main

import (
	"log"
	"net"
)

type Server struct {
	BindAddress string

	Listener net.Listener
	Logger   *log.Logger

	config *Config

	logOutput io.Writer
	opened    bool

	err     chan error
	closing chan struct{}
}

const ErrServerOpened = errors.New("Server is already opened")

func NewServer(c *Config) *Server {
	return &Server{
		Logger:      log.New(os.Stderr, "", log.LstdFlags),
		BindAddress: c.BindAddress,
		logOutput:   os.Stderr,
		config:      c,
		opened:      false,
	}
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) error {
	if s.opened {
		return ErrServerOpened
	}
	s.Logger = log.New(os.Stderr, "", log.LstdFlags)
	s.logOutput = w
	return nil
}

func (s *Server) Open() error {

	//TODO revist this later
	//updated opened at end of Open function
	s.opened = true
}

func (s *Server) Err() <-chan error { return s.err }

func (s *Server) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}

	close(s.closing)
}
