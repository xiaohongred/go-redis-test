package database

import (
	"go-redis/interface/resp"
	"strings"
)

var cmdTable = make(map[string]*command)

type ExecFunc func(db *DB, args [][]byte) resp.Reply

type command struct {
	exector ExecFunc
	arity int // 参数数量
}

func RegisterCommand(name string, exector ExecFunc, arity int)  {
	c := &command{
		exector: exector,
		arity: arity,
	}
	name = strings.ToLower(name)
	cmdTable[name] = c
}

