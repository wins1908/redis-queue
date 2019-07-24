package redisq

type message struct {
	Headers  map[string]interface{} `json:"headers"`
	Type     string                 `json:"type"`
	Attempts uint                   `json:"attempts"`
	Data     interface{}            `json:"data"`
}
