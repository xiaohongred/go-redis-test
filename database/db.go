package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

func init()  {
	RegisterCommand("ping", Ping, 1)
	RegisterCommand("del", execDel, -2)
	RegisterCommand("exists", execExists, -2) //
	RegisterCommand("flushdb", execFlushDb, -1) // flush a b c
	RegisterCommand("type", execType, 2) // type k1
	RegisterCommand("rename", execRename, 3) // rename k1 k2
	RegisterCommand("renamenx", execRenameNX, 3) // renamenx k1 k2
	RegisterCommand("keys", execKeys, 2) // keys *
}

type DB struct {
	index int
	data dict.Dict

	addAof func(line CmdLine)
}

func makeDB() *DB {
	res := &DB{
		data: dict.MakeSyncDict(),
		addAof: func(line CmdLine) {
		},
	}
	return res
}


type CmdLine [][]byte

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	// 错误处理
	if cmdLine == nil {
		return nil
	}
	//PING SET SETNX
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command" + cmdName)
	}

	// 校验参数
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	f := cmd.exector
	res := f(db, cmdLine[1:])
	return res
}

func (db *DB) Remove(key string)  {
	db.data.Remove(key)
}

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Removes(keys ...string) (deleted int)  {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Removes(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush()  {
	db.data.Clear()
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	return true
}


