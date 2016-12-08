package run

import (
	"errors"
	"io"
	"log"
	"net"
	"os"

	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/zhexuany/esm-filter/client"
	"github.com/zhexuany/esm-filter/mapreduce"
	"strconv"
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
		inputChan := make(chan interface{})
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
	responseTime float64
}

func mapper(input interface{}, output chan interface{}) {
	//parse buf as Points which defined infludb
	points, err := models.ParsePoints(input.([]byte))
	if err != nil {
		panic("failed to parse points")
	}

	o := make(map[string]RequestStatMapper)
	for _, p := range points {
		tags := p.Tags().Map()
		var status_code int64
		var err error
		var host, serverName, mapKey, path string
		for k, v := range tags {
			switch k {
			case "host":
				host = v
			case "server_name":
				serverName = v
			case "status_code":
				status_code, err = strconv.ParseInt(v, 10, 32)
				if err != nil {
					fmt.Println("failed to parse int", err)
				}
			case "path":
				path = v
			}
		}

		mapKey = host + "," + serverName + "," + path

		fields := p.Fields()
		rs := RequestStatMapper{}
		value, exists := fields["response_time"]
		if !exists {
			fmt.Printf("response_time is not in fields")
		} else {
			rs.responseTime = value.(float64)
		}
		rs.statusCode = int(status_code)
		if status_code/400 > 0 {
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
	totalResponseTime float64
	totalFailureTimes uint64
	totalRequestTimes uint64

	statusCodeMap map[int]int
}

func (rsr *RequestStatReducer) Update(value RequestStatMapper) {
	rsr.totalRequestTimes += 1
	if !value.success {
		rsr.totalFailureTimes += 1
	}
	rsr.statusCodeMap[value.statusCode] = rsr.statusCodeMap[value.statusCode] + 1
	rsr.totalResponseTime += value.responseTime
}

//map[string]RequestStatReducer
func reducer(input chan interface{}, output chan interface{}) {
	results := map[string]RequestStatReducer{}
	for matches := range input {
		for key, value := range matches.(map[string]RequestStatMapper) {
			va, exists := results[key]
			if !exists {
				rsr := RequestStatReducer{}
				rsr.statusCodeMap = make(map[int]int)
				rsr.Update(value)
				results[key] = rsr
			} else {
				va.Update(value)
				results[key] = va
			}
		}
	}

	output <- results
}
