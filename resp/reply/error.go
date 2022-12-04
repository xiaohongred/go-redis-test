package reply

type UnknowErrReply struct {

}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (u UnknowErrReply) Error() string {
	return "Err unknown"
}

func (u UnknowErrReply) ToBytes() []byte {
	return unknownErrBytes
}

type ArgNumErrReply struct {
	Cmd string
}


func (a *ArgNumErrReply) Error() string {
	return "-ERR wrong number of arguments for '" + a.Cmd + "' command"
}

func (a *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + a.Cmd + "' command\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

type SyntaxErrReply struct {

}
var syntaxErrBytes = []byte("Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

func (s SyntaxErrReply) Error() string {
	return "Err syntax error"
}

func (s SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}
func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}


// WrongTypeErrReply represents operation against a key holding the wrong kind of value
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}


// ProtocolErrReply represents meeting unexpected byte during parse requests
type ProtocolErrReply struct {
	Msg string
}

// ToBytes marshals redis.Reply
func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error: '" + r.Msg
}

