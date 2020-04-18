package main

import (
	"bufio"
	"encoding/binary"
	"epaxosproto"
	"fastrpc"
	"flag"
	"fmt"
	"genericsmrproto"
	"io"
	"log"
	"mastercontrol"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const CHAN_BUFFER_SIZE = 200000

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes *int = flag.Int("N", 3, "Number of replicas. Defaults to 3.")

// Flag to get going
var intercept *bool = flag.Bool("intercept", false, "To activate the master which intercepts all messages in the network")
var controlConf *string = flag.String("conf", "", "Config file path for the controller")

var msgTable map[uint8]fastrpc.Serializable
var msgType map[uint8]string

type Master struct {
	N            int
	nodeList     []string
	addrList     []string
	portList     []int
	lock         *sync.Mutex
	nodes        []*rpc.Client
	leader       []bool
	alive        []bool
	conn         []net.Conn
	readers      []*bufio.Reader
	writers      []*bufio.Writer
	controller   mastercontrol.Controller
	stopChan     chan int
	dispatchChan chan *mastercontrol.Message
}

func init() {
	msgTable = make(map[uint8]fastrpc.Serializable)
	count := genericsmrproto.GENERIC_SMR_BEACON_REPLY + 1

	msgTable[count] = new(epaxosproto.Prepare)
	msgTable[count+1] = new(epaxosproto.PrepareReply)
	msgTable[count+2] = new(epaxosproto.PreAccept)
	msgTable[count+3] = new(epaxosproto.PreAcceptReply)
	msgTable[count+4] = new(epaxosproto.PreAcceptOK)
	msgTable[count+5] = new(epaxosproto.Accept)
	msgTable[count+6] = new(epaxosproto.AcceptReply)
	msgTable[count+7] = new(epaxosproto.Commit)
	msgTable[count+8] = new(epaxosproto.CommitShort)
	msgTable[count+9] = new(epaxosproto.TryPreAccept)
	msgTable[count+10] = new(epaxosproto.TryPreAcceptReply)

	msgType = make(map[uint8]string)
	msgType[count] = "Prepare"
	msgType[count+1] = "PrepareReply"
	msgType[count+2] = "PreAccept"
	msgType[count+3] = "PreAcceptReply"
	msgType[count+4] = "PreAcceptOK"
	msgType[count+5] = "Accept"
	msgType[count+6] = "AcceptReply"
	msgType[count+7] = "Commit"
	msgType[count+8] = "CommitShort"
	msgType[count+9] = "TryPreAccept"
	msgType[count+10] = "TryPreAcceptReply"
}

func main() {
	flag.Parse()

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	master := &Master{*numNodes,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes),
		make([]net.Conn, *numNodes),
		make([]*bufio.Reader, *numNodes),
		make([]*bufio.Writer, *numNodes),
		nil,
		nil,
		nil,
	}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	if *intercept {
		master.controller = mastercontrol.NewPCTFileController(*numNodes, *controlConf, msgType)
		master.stopChan = make(chan int, 1)
		master.dispatchChan = make(chan *mastercontrol.Message, CHAN_BUFFER_SIZE)
		master.controller.Init(nil, master.stopChan, master.dispatchChan)

		go master.waitForReplicaConnections()
		go master.listenToStopReplica()
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) listenToStopReplica() {
	for msg := range master.stopChan {
		if master.alive[msg] {
			for done := false; !done; {
				err := master.nodes[msg].Call("Replica.ShutdownReplica", new(genericsmrproto.ShutdownArgs), new(genericsmrproto.ShutdownReply))
				if err == nil {
					done = true
					master.alive[msg] = false
				}
				time.Sleep(1e8)
			}
		}
	}
}

func (master *Master) waitForReplicaConnections() {
	var b [4]byte
	bs := b[:4]

	listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", *portnum-1))
	for i := 0; i < *numNodes; i++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			fmt.Println("Error reading id:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))

		log.Printf("Replica %d connected to master", id)

		master.conn[id] = conn
		master.readers[id] = bufio.NewReader(conn)
		master.writers[id] = bufio.NewWriter(conn)
	}

	for rid, reader := range master.readers {
		go master.replicaListener(rid, reader)
	}
}

func (master *Master) replicaListener(rid int, reader *bufio.Reader) {
	var b [4]byte
	bs := b[:4]

	var msgType uint8
	var err error = nil

	for err == nil {
		if _, err = io.ReadFull(reader, bs); err != nil {
			fmt.Println("Error reading from buffer:", err)
			break
		}
		to := int32(binary.LittleEndian.Uint32(bs))

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		// log.Printf("Recieved Message of type %d from %d to %d", msgType, rid, to)

		if objType, present := msgTable[msgType]; present {
			obj := objType.New()
			if err = obj.Unmarshal(reader); err != nil {
				break
			}
			master.controller.NotifyMessage(&mastercontrol.Message{0, rid, int(to), msgType, obj})
		}
	}
}

func (master *Master) dispatcher() {
	for msg := range master.dispatchChan {
		writer := master.writers[msg.To]
		writer.WriteByte(msg.MsgType)
		msg.Msg.Marshal(writer)
		writer.Flush()
	}
}

func (master *Master) run() {
	if *intercept {
		go master.controller.Run()
		go master.dispatcher()
	}

	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Println(err)
			log.Fatalf("Error connecting to replica %d\n", i)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

	for true {
		time.Sleep(3000 * 1000 * 1000)
		new_leader := false
		for i, node := range master.nodes {
			err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
			if err != nil {
				//log.Printf("Replica %d has failed to reply\n", i)
				master.alive[i] = false
				if master.leader[i] {
					// neet to choose a new leader
					new_leader = true
					master.leader[i] = false
				}
			} else {
				master.alive[i] = true
			}
		}
		if !new_leader {
			continue
		}
		for i, new_master := range master.nodes {
			if master.alive[i] {
				err := new_master.Call("Replica.BeTheLeader", new(genericsmrproto.BeTheLeaderArgs), new(genericsmrproto.BeTheLeaderReply))
				if err == nil {
					master.leader[i] = true
					log.Printf("Replica %d is the new leader.", i)
					break
				}
			}
		}
	}
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {

	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if nlen == master.N {
		if *intercept {
			reply.Ready = master.controller.ShallStart(index)
		} else {
			reply.Ready = true
		}
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
		reply.Slave = *intercept
	} else {
		reply.Ready = false
	}

	return nil
}

func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000)
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i}
			break
		}
	}
	return nil
}

func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}
