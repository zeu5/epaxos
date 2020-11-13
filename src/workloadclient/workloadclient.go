package main

import (
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/rpc"
	"runtime"
	"state"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")

var N int

var successful int = 0

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(2)

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)
	reqs := make(map[int]int)

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
		reqs[i] = 0
	}

	var id int32 = 0
	done := make(chan bool, N)

	args := genericsmrproto.Propose{id, state.Command{state.PUT, 21, 45}, 0}

	r := 0
	reqs[r]++

	fmt.Printf("Sending first request to replica %d\n", r)

	writers[r].WriteByte(genericsmrproto.PROPOSE)
	args.Marshal(writers[r])
	writers[r].Flush()

	args = genericsmrproto.Propose{id, state.Command{state.PUT, 21, 46}, 0}

	r = 2
	reqs[r]++

	fmt.Printf("Sending second request to replica %d\n", r)

	writers[r].WriteByte(genericsmrproto.PROPOSE)
	args.Marshal(writers[r])
	writers[r].Flush()

	for k, v := range reqs {
		go waitReplies(readers, k, v, done)
	}

	for range reqs {
		<-done
	}

	fmt.Printf("Successful %d\n", successful)

	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	master.Close()
}

func waitReplies(readers []*bufio.Reader, leader int, n int, done chan bool) {
	e := false

	reply := new(genericsmrproto.ProposeReplyTS)
	for i := 0; i < n; i++ {
		if err := reply.Unmarshal(readers[leader]); err != nil {
			fmt.Println("Error when reading:", err)
			e = true
			continue
		}
		if reply.OK != 0 {
			successful++
		}
	}
	done <- e
}
