package mastercontrol

type DummyController struct {
	dispatchChan chan *Message
	stopChan     chan int
}

func NewDummyController(numNodes int, config string, _ map[uint8]string) *DummyController {
	return &DummyController{
		nil,
		nil,
	}
}

func (d *DummyController) Init(_ map[string]string, stopChan chan int, dispatchChan chan *Message) {
	d.stopChan = stopChan
	d.dispatchChan = dispatchChan
}

func (d *DummyController) ShallStart(_ int) bool {
	return true
}

func (d *DummyController) NotifyMessage(m *Message) {
	d.dispatchChan <- m
}

func (d *DummyController) Run() {

}
