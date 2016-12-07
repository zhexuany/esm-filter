package message

type RequestStatistic struct {
	TotalRequest  uint64
	TotalFailure  uint64
	StatusCodeMap map[string]int
	TotalTime     double
}
