package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
)

type ID [20]byte
type PeerBucket [160][]ID

type MessageType int

const (
	//Tells a target peer to store a file hash and its data in its local data map.
	STORE_RES MessageType = iota
	STORE_REQ

	//Requests or responds with the file data (or the addresses of peers holding the file)
	// associated with a specific file hash
	FIND_VALUE_REQ

	//Responds with type FILE
	FIND_VALUE_RES

	//Requests the k closest nodes to a target ID.

	//this lowkey body doubles as FIND_VALUE_REQ
	FIND_NODE_REQ

	//Responds with type ID
	FIND_NODE_RES
)

type Envelope struct {
	Type        MessageType `json:"messageType"`
	Origin      string      `json:"origin"`
	ForwardedBy ID          `json:"forwardedBy"`
	Target      ID			`json:"targetID"`
	Raw         json.RawMessage `json:"raw"`
}

func (e *Envelope) SetPayload(v any) error {
	bytes, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.Raw = bytes
	return nil
}

type File struct {
	ID      ID
	Content string
}

func (e *Envelope) SendTo(conn net.Conn, v any) error {
	if err := e.SetPayload(v); err != nil {
		return err
	}
	return json.NewEncoder(conn).Encode(e)
}



type Node struct {
	ID            ID
	addr          string
	AvailableData map[[20]byte]string
	listener      net.Listener
	mu            sync.Mutex
	peers         PeerBucket
	peerAddrs     map[ID]string
}

func NewNode(id ID, addr string, peers PeerBucket) *Node {
	n := &Node{
		ID:            id,
		addr:          addr,
		peers:         peers,
		mu:            sync.Mutex{},
		AvailableData: make(map[[20]byte]string),
	}

	go n.Listen()

	return n
}

func (n *Node) Listen() {
	var err error
	n.listener, err = net.Listen("tcp", n.addr)
	if err != nil {
		fmt.Printf("Node %x failed to start : %v\n", n.ID, err)
		return
	}

	for {
		conn, err := n.listener.Accept()
		if err != nil {
			fmt.Printf("Node %x had error accepting connection %v\n", n.ID, err)
		} else {
			go n.handleIncomingRequest(conn)
		}

	}
}

func (n *Node) handleIncomingRequest(peer net.Conn) {
	defer peer.Close()
	var env Envelope
	if err := json.NewDecoder(peer).Decode(&env); err != nil {
		fmt.Printf("Node %x unable to decode connection json : %v\n", n.ID, err)
		return
	}

	switch env.Type {

	case STORE_REQ:
		n.Store(env)
		fmt.Println("Handling STORE request: saving file hash and data")
	case FIND_VALUE_REQ, FIND_NODE_REQ:
		fmt.Printf("node %x received the request to find file from peer : %s\n",n.ID,env.Origin)
		n.FindFileOrClosestPeer(env, peer)
	default:
		fmt.Printf("Unknown message type received: %d\n", env.Type)
	}
}

func(n *Node)Dial(peer ID)(net.Conn,error){
	addr:=n.getPeerAddr(peer)
	if addr==""{
		return nil,fmt.Errorf("Node %x failed to find peer with id %x",n.ID,peer)
	}
	return net.Dial("tcp",addr)
}

func (n *Node) PrepareMessage(Type MessageType, Target ID, Origin string) *Envelope {
	env := &Envelope{
		Type:        Type,
		Target:      Target,
		Origin:      Origin,
		ForwardedBy: n.ID,
	}
	fmt.Printf("node %x %x generated envelope %+v\n",n.ID,n.addr,env)
	return env
}

func(n *Node)Store(req Envelope){
	//we're optimistically assuming store_req will always contain type file and not a peer node/value ID
	//if it doesn't sender is stupid and doesn't deserve his data being stored into other nodes 
	var file File
	if err:=json.Unmarshal(req.Raw,&file);err!=nil{
		fmt.Printf("node %x failed to store file sent by node %s: %v\n",n.ID,req.Origin,err)
		return 
	}
	
	n.mu.Lock()
	if _,exist:=n.AvailableData[file.ID];!exist{
		n.AvailableData[file.ID]=file.Content
	}
	n.mu.Unlock()
	fmt.Printf("node %x stored file from %s",n.ID,req.Origin)
}

func(n *Node)ReqStore(peer ID, file File){
	conn,err:=n.Dial(peer); if err!=nil{
		fmt.Printf("%s", err.Error())
		return 
	}
	defer conn.Close()

	if err:=n.PrepareMessage(STORE_REQ,peer,n.addr).SendTo(conn,file);err!=nil{
		fmt.Printf("Node %x failed to send file to peer %x : %v\n",n.ID,peer,err)
		return 
	}
	fmt.Printf("Node %x successfully sent file to peer %x\n", n.ID, peer)
}

func (n *Node) RequestFile(target ID) {
	bucketIndex := n.findClosestBucket(target)
	if bucketIndex == -1 {
		return
	}
	peer := n.FindClosestPeerInBucket(bucketIndex, target)

	addr := n.getPeerAddr(peer)
	fmt.Printf("node id: %x, hop addr: %s, bucket index : %d\n", n.ID, addr, bucketIndex)
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("Error connecting to %s: %v\n", addr, err)
			return
		}

		// Keep tracking if we found the file to break the loop safely after conn.Close()
		env := Envelope{Type: FIND_NODE_REQ, Origin: n.addr, Target: target}
		if err := json.NewEncoder(conn).Encode(&env); err != nil {
			fmt.Printf("Error encoding envelope: %v\n", err)
			conn.Close()
			return
		}

		var resp Envelope

		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			fmt.Printf("Node %x had error decoding response: %v\n", n.ID, err)
			conn.Close()
			return
		}

		switch resp.Type {
		case FIND_NODE_RES:
			//need to query that node
			var nextHopAddr string
			if err := json.Unmarshal(resp.Raw, &nextHopAddr); err != nil {
				fmt.Printf("Error unmarshaling next hop address: %v\n", err)
				conn.Close()
				return
			}
			if nextHopAddr == addr {
				//our peer is dumb moron
				conn.Close()
				return
			}

			addr = nextHopAddr
			fmt.Printf("Hopping to next closer peer address: %s\n", addr)
		case FIND_VALUE_RES:
			//found the file, store it in map
			var file File
			if err := json.Unmarshal(resp.Raw, &file); err != nil {
				fmt.Printf("Error unmarshaling file data: %v\n", err)
				conn.Close()
				return
			}
			n.mu.Lock()
			n.AvailableData[file.ID] = file.Content
			n.mu.Unlock()
			conn.Close()
			return
		default:
			//what a useless node?
			fmt.Printf("what a useless node? \n")
		}

		//  close the socket for the current hop before looping or returning
		conn.Close()

		time.Sleep(1 * time.Second)
		fmt.Printf("next addr: %s\n", addr)
	}
}
func (n *Node) FindFileOrClosestPeer(req Envelope, conn net.Conn) {
	n.mu.Lock()
	fmt.Printf("curr node : %x\n",n.ID)
	if f, exist := n.AvailableData[req.Target]; exist {
		fileToSend := File{
        	ID:      req.Target,
        	Content: f,
    	}
		if err := n.PrepareMessage(FIND_VALUE_RES, req.Target, req.Origin).SendTo(conn, fileToSend); err != nil {
			fmt.Printf("Node %x had error sending message to node %s : %v\n", n.ID, req.Origin, err)
			n.mu.Unlock()
			return
		}
		fmt.Printf("Node %x found the file\n", n.ID)
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	bucketIndex := n.findClosestBucket(req.Target)
	if bucketIndex == -1 {
		n.PrepareMessage(FIND_NODE_RES,req.Target,req.Origin).SendTo(conn,"")
		return
	}
	peer := n.FindClosestPeerInBucket(bucketIndex, req.Target)
	//in real world scenario the peer address map might be updated before the address is read in this goroutine
	//leaving us with an empty or outdated string, so a function wide lock might be better
	//but for this program, we're leaving it as it is.
	addr := n.getPeerAddr(peer)
	if err := n.PrepareMessage(FIND_NODE_RES, req.Target, req.Origin).SendTo(conn, addr); err != nil {
		fmt.Printf("Node %x had error sending message to node %s : %v\n", n.ID, req.Origin, err)
		return
	}
	fmt.Printf("Node %x found a closer peer : %s\n", n.ID, addr)
}

func (n *Node) FillBucket(peers []ID) {
	for _, peer := range peers {
		distance := n.getBucketIndex(peer)
		if distance != -1 {
			n.peers[distance] = append(n.peers[distance], peer)
		}
	}                                     
}

func (n *Node) getBucketIndex(peer ID) int {
	leadingZeroes := 0

	for i := range 20 {
		xorDistance := n.ID[i] ^ peer[i]

		if xorDistance == byte(0) {
			leadingZeroes += 8
			continue
		}

		for mask := byte(128); mask > 0; mask >>= 1 {
			if xorDistance&mask == 0 {
				leadingZeroes++
			} else {
				break
			}
		}
		// Invert the index so larger distances get larger bucket indices
		return 159 - leadingZeroes
	}
	return -1
}

func (n *Node) nonEmptyBucketIndexes() []int {
	var nonEmptyBuckets []int
	for i, bucket := range n.peers {
		if len(bucket) != 0 {
			nonEmptyBuckets = append(nonEmptyBuckets, i)
		}
	}
	return nonEmptyBuckets
}

func (n *Node) findClosestBucket(filehash ID) int {
	fileBucketIndex := n.getBucketIndex(filehash)
	if len(n.peers[fileBucketIndex]) != 0 {
		return fileBucketIndex
	}
	nonEmptyBuckets := n.nonEmptyBucketIndexes()
	size := len(nonEmptyBuckets)
	if size == 0 {
		return -1
	}

	if fileBucketIndex <= nonEmptyBuckets[0] {
		return nonEmptyBuckets[0]
	}

	if fileBucketIndex >= nonEmptyBuckets[size-1] {
		return nonEmptyBuckets[size-1]
	}

	left, right := 0, size-1

	for left < right {
		mid := left + (right-left)/2

		if fileBucketIndex < nonEmptyBuckets[mid] {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if (fileBucketIndex - nonEmptyBuckets[right]) < (nonEmptyBuckets[left] - fileBucketIndex) {
		return nonEmptyBuckets[right]
	}
	return nonEmptyBuckets[left]
}

func (n *Node) FindClosestPeerInBucket(index int, file ID) ID {
	peers := n.peers[index]
	slices.SortFunc(peers, func(a, b ID) int {
		var distA, distB [20]byte
		for i := range 20 {
			distA[i] = a[i] ^ file[i]
			distB[i] = b[i] ^ file[i]
		}
		return bytes.Compare(distA[:], distB[:])
	})
	//actual k-dht returns 20 nearest peers but for this program
	//we're only returning the 1st one
	return peers[0]
}

func (n *Node) getPeerAddr(id ID) string {
	n.mu.Lock()
	addr := n.peerAddrs[id]
	n.mu.Unlock()
	return addr
}


func generateID(key string) ID {
	hash := sha256.Sum256([]byte(key))
	var id ID
	copy(id[:], hash[:20])
	return id
}
func main() {
	// Node 1: Filled with 1s
	var id1 ID
	for i := range id1 { id1[i] = 1 }

	// Node 2: 1 at index 7
	var id2 ID
	id2[7] = 1

	// Node 3: 1 at index 8
	var id3 ID
	id3[8] = 1

	// Node 4: 1 at index 9
	var id4 ID
	id4[9] = 1

	// fileID: Simple random byte identifier for the STORE request
	var fileID ID
	fileID[0] = 99 

	// file2ID: Shares its 1 at index 8, making it closer to Node 3
	var file2ID ID
	file2ID[8] = 1
	file2ID[19] = 55 // Small variance at the end

	// file3ID: Shares its 1 at index 9, making it closer to Node 4
	var file3ID ID
	file3ID[9] = 1
	file3ID[19] = 77 // Small variance at the end

	fmt.Printf("Node 1 ID: %x\n", id1)
	fmt.Printf("Node 2 ID: %x\n", id2)
	fmt.Printf("Node 3 ID: %x\n", id3)
	fmt.Printf("Node 4 ID: %x\n", id4)
	fmt.Printf("File ID:   %x\n", fileID)
	fmt.Printf("File 2 ID: %x\n", file2ID)
	fmt.Printf("File 3 ID: %x\n\n", file3ID)

	// Initialize Nodes
	n1 := NewNode(id1, "127.0.0.1:8001", PeerBucket{})
	n2 := NewNode(id2, "127.0.0.1:8002", PeerBucket{})
	n3 := NewNode(id3, "127.0.0.1:8003", PeerBucket{})
	n4 := NewNode(id4, "127.0.0.1:8004", PeerBucket{})

	n3.AvailableData[file2ID] = "tester"
	n4.AvailableData[file3ID] = "rander"
	
	n1.peerAddrs = make(map[ID]string)
	n2.peerAddrs = make(map[ID]string)
	n3.peerAddrs = make(map[ID]string)
	n4.peerAddrs = make(map[ID]string)

	n1.FillBucket([]ID{id2})
	n1.peerAddrs[id2] = n2.addr

	n2.FillBucket([]ID{id3, id4})
	n2.peerAddrs[id3] = n3.addr
	n2.peerAddrs[id4] = n4.addr

	n3.FillBucket([]ID{id4})
	n3.peerAddrs[id4] = n4.addr

	n4.FillBucket([]ID{id3})
	n4.peerAddrs[id3] = n3.addr

	time.Sleep(100 * time.Millisecond)

	// Test Store routine
	testFile := File{
		ID:      fileID,
		Content: "Hello, this is secret decentralized data!",
	}
	n1.ReqStore(id2, testFile) 
	time.Sleep(100 * time.Millisecond)

	// Request targets sequentially
	n1.RequestFile(file2ID)
	time.Sleep(500 * time.Millisecond)

	n1.RequestFile(file3ID)
	time.Sleep(500 * time.Millisecond)

	// Pull results
	n1.mu.Lock()
	content1, found1 := n1.AvailableData[file2ID]
	content2, found2 := n1.AvailableData[file3ID]
	n1.mu.Unlock()

	fmt.Println("\n--- Result ---")
	if found1 && found2 {
		fmt.Printf("Success! Node 1 successfully retrieved data: %q and %q\n", content1, content2)
	} else {
		fmt.Println("Failure: Node 1 could not find the files.")
		fmt.Println("found1 (Node 3 file):", found1, "| found2 (Node 4 file):", found2)
	}

	// Clean up listeners safely
	n1.listener.Close()
	n2.listener.Close()
	n3.listener.Close()
	n4.listener.Close()
}