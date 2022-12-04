package parser

import (
	"go-redis/interface/resp"
	"io"
)

type PayLoad struct {
	Data resp.Reply
	Err error
}

type readState struct {
	readingMultiLine bool
	expectedArgsCount int
	msgType byte
	args [][]byte
	bulkLen int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *PayLoad {
	ch := make(chan *PayLoad)

	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *PayLoad)  {

}