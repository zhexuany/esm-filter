package run

import (
	"errors"
	"io"
	"log"
	"net"
	"os"

	"github.com/influxdata/influxdb/models"
	"github.com/zhexuany/esm-filter/client"
	"github.com/zhexuany/esm-filter/mapreduce"
)

type Server struct {
	BindAddress string

	Listener net.Listener
	Logger   *log.Logger

	config *client.Config

	client *client.Client

	logOutput io.Writer
	opened    bool

	err     chan error
	closing chan struct{}
}

var ErrServerOpened = errors.New("Server is already opened")

func NewServer(c *client.Config) *Server {
	return &Server{
		Logger:      log.New(os.Stderr, "", log.LstdFlags),
		BindAddress: c.BindAddress,
		logOutput:   os.Stderr,
		config:      c,
		opened:      false,
		client:      client.NewClient(c),
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
	s.client.Open()
	return nil
}

func (s *Server) Run() {
	for {
		s.client.Read()
		go mapreduce.MapReduce(mapper, reducer, inputChan)
	}
}

func (s *Server) Err() <-chan error { return s.err }

func (s *Server) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}

	close(s.closing)
	return nil
}

type RequestStatMapper struct {
	success      bool
	statusCode   int
	responseTime double
}

func mapper(input []byte, output chan map[string]RequestStatMapper) {
	//parse buf as Points which defined infludb
	points := models.ParsePoints([]byte(input))

	o := make(map[string]RequestStatMapper)
	for i, p := range points {
		tags := p.Tags().Map()
		var status_code int
		var mapKey string
		for k, v := range tags {
			switch k {
			case "host":
				mapKey = v + ","
			case "server_name":
				mapKey = v + ","
			case "status_code":
				status_code = v
			case "path":
				mapKey = v
			}
		}

		fields := p.Fields()
		rs := &RequestStatMapper{}
		rs.responseTime = fields["response_time"]
		if status_code/400 > 1 {
			rs.success = false
		} else {
			rs.success = true
		}
		o[mapKey] = rs
	}
	output <- o
	//requests,host=$hostname,server_name=$host,path=$uri total_request_times=$request_number, total_failure_times=$failure_times,total_response_time=$request_time,%s, ${udp_usec}000
}

type RequestStatReducer struct {
	totalResponseTime double
	totalFailureTimes uint64
	totalRequestTimes uint64

	statusCodeMap map[int]int
}

func reducer(input chan map[string]RequestStatMapper, output chan map[string]RequestStatReducer) {
	outMap := make(map[string]RequestStatReducer)
	for k, v := range input {
		rsr := outMap[k]
		if rsr != nil {
			rsr.totalRequestTimes += 1
			if !v.success {
				rsr.totalFailureTimes += 1
			}
			rsr.totalResponseTime += v.responseTime
			rsr.statusCodeMap[v.statusCode] = rsr.statusCodeMap[v.statusCode] + 1
		} else {
			rsr := RequestStatReducer{}
			rsr.statusCodeMap = make(map[int]int)
			rsr.totalRequestTimes += 1
			if !v.success {
				rsr.totalFailureTimes += 1
			}
			rsr.totalResponseTime += v.responseTime
			rsr.statusCodeMap[v.statusCode] = rsr.statusCodeMap[v.statusCode] + 1
			outMap[k] = rsr
		}
	}

	output <- outMap
}
