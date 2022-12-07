package database

import (
	"fmt"
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

// Database is a set of multiple database set
type Database struct {
	dbSet []*DB
	aofHandler *aof.AofHandler
}

// NewDatabase creates a redis database,
func NewDatabase() *Database {
	mdb := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}

	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(mdb)
		if err != nil {
			panic(err)
		}
		for _, db := range mdb.dbSet {
			// avoid closure
			// db = dbSet[0]
			// db = dbSet[1] ......       最后， db=dbSet[15], 最后， 所有的db.index 都是15
			signalDb := db
			signalDb.addAof = func(line CmdLine) { // 这个匿名方法变成了一个闭包，我们执行这个循环的时候，循环了16次，
				mdb.aofHandler.AddAof(signalDb.index, line)
			}
		}
		mdb.aofHandler = aofHandler
	}

	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (mdb *Database) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close graceful shutdown database
func (mdb *Database) Close() {

}

func (mdb *Database) AfterClientClose(c resp.Connection) {
}

func execSelect(c resp.Connection, mdb *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
