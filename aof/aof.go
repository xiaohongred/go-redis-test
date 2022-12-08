package aof

import (
	"go-redis/config"
	"go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
)

const  aofBufSize = 1 << 16

type payload struct {
	cmdLine [][]byte
	dbIndex int
}

type AofHandler struct {
	database database.Database
	aofChan chan *payload
	aofFile *os.File
	aofFilename string
	currentDB int
}


// NewAofHandler
func NewAofHandler(database database.Database) (*AofHandler, error) {
	aofHand := &AofHandler{
		database: database,
		aofFilename: config.Properties.AppendFilename,
	}
	aofHand.LoadAof()
	file, err := os.OpenFile(config.Properties.AppendFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	aofHand.aofFile = file
	aofHand.aofChan = make(chan *payload, aofBufSize)
	go func() {
		aofHand.handleAof()
	}()

	return aofHand, nil
}



// Add  plyload(set k v) --> aofChan
func (handler *AofHandler) AddAof(dbIndex int, cmd database.CmdLine)  {

	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}

}

// handleAof       payload(set k v) <- aofChan(落盘)
func (handler *AofHandler) handleAof() {
	//TODO: payload(set k v) <- aofChan(落盘)
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			dataByte := reply.MakeMultiBulkReply(utils.ToCmdLine("select", string(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(dataByte)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}

		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
			continue
		}
	}
}

// LoadAof
func (handler *AofHandler) LoadAof()  {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}

		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need multi bulk")
			continue
		}

		rep := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(rep) {
			logger.Error(rep)
		}
	}
}