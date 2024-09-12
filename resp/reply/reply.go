package reply

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}
