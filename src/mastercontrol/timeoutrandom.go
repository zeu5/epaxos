package mastercontrol

type TimeoutControl struct {
	messagePool  map[int64]*Message
	dispatchChan chan *Message
}
