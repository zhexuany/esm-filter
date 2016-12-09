package run

import (
	"io"
	"log"
	"os"

	"fmt"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/zhexuany/esm-filter/client"
	"github.com/zhexuany/esm-filter/mapreduce"
	"strconv"
	"strings"
	"time"
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
}

func NewServer(c *client.Config) *Server {
	w := NewSimplerWriter(c.Downstream)
	return &Server{
		Logger:      log.New(os.Stderr, "", log.LstdFlags),
		BindAddress: c.BindAddress,
		err:         make(chan error),
		closing:     make(chan struct{}),
		logOutput:   os.Stderr,
		client:      client.NewClient(c),
		ticker:      time.NewTicker(c.Ticket),
		downstream:  c.Downstream,
		w:           w,
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
	if err := s.client.Open(); err != nil {
		return nil
	}

	return nil
}

func (s *Server) Run() {
	for {
		var inputChan chan interface{}
		go func() {
			for {
				inputChan = make(chan interface{})
				for {
					select {
					//got a tick, break it
					case <-s.ticker.C:
						close(inputChan)
						s.w.write(s.points)
						break
					default:
						//keep reading until receive a tick
						buf, err := s.client.Read()
						if err != nil {
							s.logOutput.Write([]byte("failed to read udp packet"))
						} else {
							inputChan <- buf
						}
					}
				}
			}
		}()

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

		}()
	}
}

type writer interface {
	write(interface{})
}

type simpleWriter struct {
	UDPConfig influxDBClient.UDPConfig
	UDPClient influxDBClient.Client
}

func NewSimplerWriter(url string) *simpleWriter {
	udpConfig := influxDBClient.UDPConfig{
		Addr: url,
	}
	udpClient, err := influxDBClient.NewUDPClient(udpConfig)
	if err != nil {
		fmt.Println("failed to create UDPClient")
	}
	return &simpleWriter{
		UDPClient: udpClient,
	}
}

func (sw *simpleWriter) write(data interface{}) {
	if bp, ok := data.(influxDBClient.BatchPoints); ok {
		sw.UDPClient.Write(bp)
	}
}

func (s *Server) Err() <-chan error { return s.err }

func (s *Server) Close() error {
	if s.client != nil {
		return s.client.Close()
	}

	close(s.closing)
	return nil
}

//TODO think about where to palce mapreduce method code
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
