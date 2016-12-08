package run

import (
	"fmt"
	"github.com/zhexuany/esm-filter/mapreduce"
	"math"
	"testing"
)

func TestServer_MapReduce(t *testing.T) {
	testKey := "qcr-web-proxy-66,restapi.ele.me,/ping"
	requestTime := 10
	responseTime := 0.001
	test := "requests,host=qcr-web-proxy-66,upstream=127.0.0.1:8444,status_code=503,server_name=restapi.ele.me,method=GET,path=/ping response_time=0.001,response_size=227 1481175443530312000"

	inputChan := make(chan interface{})

	go func() {
		for i := 0; i < requestTime; i++ {
			inputChan <- []byte(test)
		}
		close(inputChan)
	}()
	fmt.Println("start mapreduce")
	results := mapreduce.MapReduce(mapper, reducer, inputChan)
	fmt.Println("finished mapreduce")

	if res, ok := results.(map[string]RequestStatReducer); ok {
		for key, value := range res {
			if key != testKey {
				t.Error("MapReduce does not work")
			}
			if value.totalFailureTimes != uint64(requestTime) {
				t.Errorf("MapReduce does not work. Expected %d but ound %d", uint64(requestTime), value.totalFailureTimes)
			}
			if value.totalRequestTimes != uint64(requestTime) {
				t.Errorf("MapReduce does not work. Expected %d but ound %d", uint64(requestTime), value.totalRequestTimes)
			}

			var EPSILON float64 = 0.00000001
			if math.Abs(value.totalResponseTime-float64(requestTime)*responseTime) > EPSILON {
				t.Errorf("MapReduce does not work. Expected %f but ound %f", float64(requestTime)*responseTime, value.totalResponseTime)
			}
		}
	}
}
