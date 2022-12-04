package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// del
// exists
// keys
// flushdb
// typee
// rename
// renamenx


func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)

	return reply.MakeIntReply(int64(deleted))
}

func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

func execFlushDb(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	return reply.MakeOkReply()
}

func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}
	return reply.UnknowErrReply{}
}

func execRename(db *DB, args [][]byte) resp.Reply {
	key1 := string(args[0])
	key2 := string(args[1])

	entity, ok := db.GetEntity(key1)
	if !ok {
		return reply.MakeErrReply("no such key " + string(key1))
	}
	db.PutEntity(key2, entity)
	db.Remove(key1)
	return reply.MakeOkReply()
}

func execRenameNX(db *DB, args [][]byte) resp.Reply {
	key1 := string(args[0])
	key2 := string(args[1])

	entity, ok := db.GetEntity(key1)
	if !ok {
		return reply.MakeErrReply("no such key " + string(key1))
	}
	_, okKey2 := db.GetEntity(key2)
	if okKey2 {
		return reply.MakeIntReply(0)
	}
	db.PutEntity(key2, entity)
	db.Remove(key1)
	return reply.MakeIntReply(1)
}

func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}












