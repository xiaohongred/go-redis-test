package reply

type PongReply struct {

}

var pongbytes = []byte("+PONG\r\n")

func (p *PongReply) ToBytes() []byte {
	return pongbytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

type OkReply struct {
	
}

var okBytes = []byte("+OK\r\n")

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

func MakeOkReply() *OkReply {
	return theOkReply
}

type NullBulkReply struct {

}

var nullBulkBytes = []byte("$-1\r\n")
var theNullBulk = new(NullBulkReply)
func (n NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return theNullBulk
}

type EmptyMultiBulkReply struct {

}
var emptyMultiBulkBytes = []byte("*0\r\n")

func (e EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

type NoReply struct {

}

var noBytes = []byte("")

func (n NoReply) ToBytes() []byte {
	return noBytes
}













