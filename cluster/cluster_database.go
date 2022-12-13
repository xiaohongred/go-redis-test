package cluster

import (
	"context"
	"fmt"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistanthash"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strings"
)

type ClusterDatabase struct {
	self string
	nodes []string
	peerPicker *consistanthash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	cluster :=  &ClusterDatabase{
		self: config.Properties.Self,
		db: database2.NewStandaloneDatabase(),
		peerPicker: consistanthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool, len(config.Properties.Peers)),
	}
	nodes := make([]string, 0, len(config.Properties.Peers))
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, cluster.self)
	cluster.peerPicker.AddNode(nodes...)
	cluster.nodes = nodes
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

var router = makeRouter()



func (cluster *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknowErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "' or not support in cluster mode")
	}
	result = cmdFunc(cluster, client, args)
	return
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}


