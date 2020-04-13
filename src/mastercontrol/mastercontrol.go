package mastercontrol

import "fastrpc"

type Message struct {
	ID      int
	From    int
	To      int
	MsgType uint8
	Msg     fastrpc.Serializable
}

type Controller interface {
	// To initialize the controller with params, list of replica ids,
	// A channel to push replica ids which should be shutdown
	// A channle to dispatch messages
	Init(map[string]string, chan int, chan *Message)

	// A function which returns if the given replica can start
	ShallStart(int) bool

	// To notify the controller new messages
	NotifyMessage(*Message)

	// To start the main loop of the controller
	Run()
}
