package run

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/zhexuany/esm-filter/client"
	"github.com/zhexuany/esm-filter/mapreduce"
)

type Server struct {
	BindAddress string

	Logger *log.Logger

	client *client.Client

	logOutput io.Writer

	points influxDBClient.BatchPoints

	err     chan error
	closing chan struct{}

	ticker *time.Ticker

	w writer

	downstream string

	BPConfig influxDBClient.BatchPointsConfig
}

func NewServer(c *client.Config) *Server {
	w, err := NewSimplerWriter(c.Downstream)
	if err != nil {
		return nil
	}
	//TODO need add this in config
	BPConfog := influxDBClient.BatchPointsConfig{
		Precision:        "s",
		Database:         "sla",
		RetentionPolicy:  "",
		WriteConsistency: "one",
	}

	return &Server{
		Logger:      log.New(os.Stderr, "", log.LstdFlags),
		BindAddress: c.BindAddress,
		err:         make(chan error),
		closing:     make(chan struct{}),
		logOutput:   os.Stderr,
		client:      client.NewClient(c),
		ticker:      time.NewTicker(c.Ticket * time.Second),
		downstream:  c.Downstream,
		w:           w,
		BPConfig:    BPConfog,
	}
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) error {
	s.Logger = log.New(os.Stderr, "", log.LstdFlags)
	s.logOutput = w
	return nil
}

// Open is a function which open server instance.
func (s *Server) Open() error {
	s.points, _ = influxDBClient.NewBatchPoints(s.BPConfig)
	if err := s.client.Open(); err != nil {
		return fmt.Errorf("failed to open udpClient to read", err)
	}
	return nil
}

// Run will keep read from port and buffer the results
func (s *Server) Run() {
	stopChan := make(chan bool, 1)
	inputChan := make(chan interface{})
	go s.filter(inputChan, stopChan)
	for _ = range s.ticker.C {
		stopChan <- true
		stopChan = make(chan bool, 1)
		inputChan = make(chan interface{})
		go s.filter(inputChan, stopChan)
	}
}

func (s *Server) filter(inputChan chan interface{}, stopChan chan bool) {
	go func() {
		results := mapreduce.MapReduce(mapper, reducer, inputChan)
		//every key and value is a point
		if res, ok := results.(map[string]RequestStatReducer); ok {
			for key, value := range res {
				tags := make(map[string]string)
				tagValueStr := strings.Split(key, ",")
				if len(tagValueStr) == 4 {
					tags["host"] = tagValueStr[1]
					tags["server_name"] = tagValueStr[2]
					tags["path"] = tagValueStr[3]
				}

				p, err := influxDBClient.NewPoint(tagValueStr[0], tags, value.Fields(), time.Now().UTC())
				if err != nil {
					s.logOutput.Write([]byte("failed to parse points"))
				}
				s.points.AddPoint(p)
			}
		}

		bp := s.points
		go s.w.write(bp)
		s.points, _ = influxDBClient.NewBatchPoints(s.BPConfig)
	}()

	//keep read until inputChan is nil
	for {
		buf, err := s.client.Read()
		if err != nil {
			s.logOutput.Write([]byte(err.Error()))
		} else {
			select {
			case _, ok := <-stopChan:
				//if stopChan is received, just terminate this goroutine
				//otherwise, we keep send buf to inputChan
				if ok {
					return
				}
			case inputChan <- buf:
			}
		}
	}
}

var (
	ErrFailedWrite           = errors.New("failed to write\n")
	ErrFailedCreateUDPClient = errors.New("failed to create UDPClient\n")
)

type writer interface {
	write(interface{}) error
}

type simpleWriter struct {
	UDPConfig influxDBClient.UDPConfig
	UDPClient influxDBClient.Client
}

func NewSimplerWriter(url string) (*simpleWriter, error) {
	udpConfig := influxDBClient.UDPConfig{
		Addr: url,
	}
	udpClient, err := influxDBClient.NewUDPClient(udpConfig)
	if err != nil {
		return nil, err
		// fmt.Println("failed to create UDPClient")
	}
	return &simpleWriter{
		UDPClient: udpClient,
	}, nil
}

func (sw *simpleWriter) write(data interface{}) error {
	if bp, ok := data.(influxDBClient.BatchPoints); ok {
		sw.UDPClient.Write(bp)
	} else {
		// fmt.Println("failed to write")
		return ErrFailedWrite
	}
	return nil
}

func (s *Server) Err() <-chan error { return s.err }

func (s *Server) Close() error {
	if s.client != nil {
		return s.client.Close()
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
		var host, serverName, mapKey, path, measurement string
		measurement = p.Name()
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

		mapKey = measurement + "," + host + "," + serverName + "," + path

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
}

type RequestStatReducer struct {
	fields map[string]interface{}
}

func (rsr *RequestStatReducer) Update(value RequestStatMapper) {
	if _, existed := rsr.fields["totalRequestTimes"]; !existed {
		rsr.fields["totalRequestTimes"] = uint64(1)
	} else {
		if val, ok := rsr.fields["totalRequestTimes"].(uint64); ok {
			rsr.fields["totalRequestTimes"] = val + 1
		}
	}

	if !value.success {
		if _, existed := rsr.fields["totalFailureTimes"]; !existed {
			rsr.fields["totalFailureTimes"] = uint64(1)
		} else {
			if val, ok := rsr.fields["totalFailureTimes"].(uint64); ok {
				rsr.fields["totalFailureTimes"] = val + 1
			}
		}
	}

	codeStr := fmt.Sprintf("%d", value.statusCode)
	if _, existed := rsr.fields[codeStr]; !existed {
		rsr.fields[codeStr] = uint64(1)
	} else {
		if val, ok := rsr.fields[codeStr].(uint64); ok {
			rsr.fields[codeStr] = val + 1
		}
	}

	if _, existed := rsr.fields["totalResponseTime"]; !existed {
		rsr.fields["totalResponseTime"] = float64(value.responseTime)
	} else {
		if val, ok := rsr.fields["totalResponseTime"].(float64); ok {
			rsr.fields["totalResponseTime"] = val + value.responseTime
		}
	}
}

func (rsr *RequestStatReducer) Fields() map[string]interface{} {
	return rsr.fields
}

//map[string]RequestStatReducer
func reducer(input chan interface{}, output chan interface{}) {
	results := map[string]RequestStatReducer{}
	for matches := range input {
		for key, value := range matches.(map[string]RequestStatMapper) {
			va, exists := results[key]
			if !exists {
				rsr := RequestStatReducer{}
				rsr.fields = make(map[string]interface{})
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
