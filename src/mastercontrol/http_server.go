package mastercontrol

import "sync"

type Server struct {
	port           string
	address        string
	messagePool    map[int]*Message
	enabled        []bool
	active         []bool
	stopChan       chan int
	dispatchChan   chan *Message
	numReplicas    int
	replicaIds     []int
	messageCounter int
	lock           *sync.Mutex
}

func NewServerController(numNodes int) *Server {
	return &Server{
		"",
		"",
		make(map[int]*Message),
		make([]bool, numNodes),
		make([]bool, numNodes),
		nil,
		nil,
		numNodes,
		make([]int, numNodes),
		0,
		new(sync.Mutex),
	}
}

func (server *Server) Init(
	params map[string]string,
	stopChan chan int,
	dispatchChan chan *Message,
) {
	server.lock.Lock()
	defer server.lock.Unlock()

	if port, exists := params["port"]; exists {
		server.port = port
	}
	if address, exists := params["address"]; exists {
		server.address = address
	}

	server.stopChan = stopChan
	server.dispatchChan = dispatchChan

	for i := 0; i < server.numReplicas; i++ {
		server.replicaIds[i] = i
		server.active[i] = false
		server.enabled[i] = false
	}
}

func (server *Server) ShallStart(replicaId int) bool {
	return server.enabled[replicaId]
}

func (server *Server) NotifyMessage(message *Message) error {
	server.lock.Lock()
	defer server.lock.Unlock()

	server.messagePool[server.messageCounter] = message
	server.messageCounter = server.messageCounter + 1
	return nil
}

func (server *Server) Run() {

}
