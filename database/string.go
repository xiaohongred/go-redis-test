package database

import (
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func init()  {
	RegisterCommand("Get", execGet, 2) // get k1
	RegisterCommand("SET", execSet, 3) // set k1 v1
	RegisterCommand("setnx", execSetNX, 3)
	RegisterCommand("getset", execGetSet, 3)
	RegisterCommand("strlen", execStrLen, 2)
}

// GET
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}

	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}


// SET k v
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	val := args[1]

	entity := &database.DataEntity{
		Data: val,
	}
	db.PutEntity(key, entity)
	return reply.MakeOkReply()
}

// SETNX
func execSetNX(db *DB, args [][]byte) resp.Reply{
	key := string(args[0])
	val := args[1]

	entity := &database.DataEntity{
		Data: val,
	}
	absent := db.PutIfAbsent(key, entity)
	return reply.MakeIntReply(int64(absent))
}

// GETSET     k1  v1  返回k1原来的值
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	valNew := args[1]

	entity, exists := db.GetEntity(key)

	newEntity := &database.DataEntity{
		Data: valNew,
	}
	db.PutEntity(key, newEntity)

	if !exists {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(entity.Data.([]byte))
}

// STRLEN
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

