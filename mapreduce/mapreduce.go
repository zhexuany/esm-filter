package mapreduce

// MapperCollector is a channel that collects the output from mapper tasks
type MapperCollector chan chan interface{}

// MapperFunc is a function that performs the mapping part of the MapReduce job
type MapperFunc func(interface{}, chan interface{})

// ReducerFunc is a function that performs the reduce part of the MapReduce job
type ReducerFunc func(interface{}, chan interface{})

func mapperDispatcher(mapper MapperFunc, input chan interface{}, collector MapperCollector) {
	for item := range input {
		taskOutput := make(chan interface{})
		go mapper(item, taskOutput)
		collector <- taskOutput
	}

	close(collector)
}

// reduceDispatcher is responsible to listen on the collector channel and push each item
// as input as Reducer task
func reduceDispatcher(collector MapperCollector, reducerInput chan interface{}) {
	for output := range collector {
		reducerInput <- <-output
	}
	close(reducerInput)
}

const (
	MaxWorkers = 10
)

func MapReduce(mapper MapperFunc, reducer ReducerFunc, input chan interface{}) interface{} {
	reducerInput := make(chan interface{})
	reducerOutput := make(chan interface{})
	mapperCollector := make(MapperCollector, MaxWorkers)

	go reducer(reducerInput, reducerOutput)
	go reduceDispatcher(mapperCollector, reducerInput)
	go mapperDispatcher(mapper, input, mapperCollector)

	return <-reducerOutput
}
