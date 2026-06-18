package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type Message struct {
	ID        	string    `json:"ID"`
	Value     	string    `json:"value"`
	Timestamp 	time.Time `json:"timestamp"`
	ForwardedBy	string	  `json:"forwardedBy"`
	Origin		string	  `json:"origin"`
}

type Node struct {
	ID       string
	addr     string
	listener net.Listener
	knowData map[string]Message
	peers    []string
	mu       sync.Mutex
}

func NewNode(ID, addr string, peers []string) *Node {
	node := &Node{
		ID:       ID,
		addr:     addr,
		peers:    peers,
		knowData: make(map[string]Message),
		mu:       sync.Mutex{},
	}
	go node.listen()
	return node
}

func (n *Node) listen() {
	var err error
	n.listener, err = net.Listen("tcp", n.addr)
	if err != nil {
		log.Printf("Node %s unable to listen : %s\n", n.ID, err.Error())
		return
	}
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			log.Printf("Node %s unable to accept connection : %s\n", n.ID, err.Error())
		} else {
			go n.handleIncomingConnection(conn)
		}
	}
}

func (n *Node) handleIncomingConnection(conn net.Conn) {
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		log.Printf("Node %s failed to read msg json : %s\n", n.ID, err.Error())
		return
	}
	log.Printf("Node %s accepted connection from : %s for message originated from %s\n", n.ID, msg.ForwardedBy,msg.Origin)
	n.mu.Lock()
	if _, exist := n.knowData[msg.ID]; exist {
		n.mu.Unlock()
		conn.Close()
	} else {
		n.knowData[msg.ID] = msg
		n.mu.Unlock()
		conn.Close()
		go n.gossip(msg)
	}
}

func (n *Node) gossip(msg Message) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	msg.ForwardedBy=n.ID
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, peer := range n.peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, "tcp", peer)
			if err != nil {
				log.Printf("node %s unable to dial node %s\n", n.ID, peer)
				return
			}
			defer conn.Close()

			if deadline, ok := ctx.Deadline(); ok {
				if err := conn.SetWriteDeadline(deadline); err != nil {
					log.Printf("node %s failed to set write deadline for %s: %v\n", n.ID, peer, err)
					return
				}
			}

			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				log.Printf("an error occurred when node %s tried to encode msg, msg id: %s msg: %v\n", n.ID, msg.ID, msg)
				return
			}
			log.Printf("node %s successfully gossiped msg id: %s to node %s\n", n.ID, msg.ID, peer)
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Msg %s successfully gossiped to all available peers by node %s\n", msg.ID, n.ID)
	case <-ctx.Done():
		log.Printf("timed out for %s \n", n.ID)
	}
}

func (n *Node)BroadCast(value string){
	b:= make([]byte,16); rand.Read(b);
	msg:=Message{
		ID: hex.EncodeToString(b),
		Value: value,
		Origin: n.ID,
		ForwardedBy: n.ID,
		Timestamp: time.Now(),
	}

	n.mu.Lock()
	n.knowData[msg.ID]=msg
	n.mu.Unlock()
	log.Printf("[%s] Initiating DKG broadcast of my public values: %v\n", n.ID, value)
	n.gossip(msg)
}

func main() {
    log.Println("Initializing a 5-node mesh network...")

    addrA := "127.0.0.1:9001"
    addrB := "127.0.0.1:9002"
    addrC := "127.0.0.1:9003"
    addrD := "127.0.0.1:9004"
    addrE := "127.0.0.1:9005"
	
    nodeA := NewNode("Node-A", addrA, []string{addrB, addrC}) // Spreads to B and C
    nodeB := NewNode("Node-B", addrB, []string{addrD, addrE}) // Spreads to D and E
    nodeC := NewNode("Node-C", addrC, []string{addrD})        // Spreads to D
    nodeD := NewNode("Node-D", addrD, []string{addrE, addrA}) // Spreads to E and back to A (Loop!)
    nodeE := NewNode("Node-E", addrE, []string{})             // End of the line

    network := []*Node{nodeA, nodeB, nodeC, nodeD, nodeE}

    time.Sleep(1 * time.Second) 

    log.Println("\n--- Starting Gossip Broadcast ---")
    nodeA.BroadCast("Secret DKG Key Share XYZ123")

    time.Sleep(3 * time.Second) 

    log.Println("\n--- Final Network State Verification ---")
    
    for _, node := range network {
        node.mu.Lock()
        numMessages := len(node.knowData)
        
        fmt.Printf("[%s] has %d message(s) in its data pool.\n", node.ID, numMessages)
        for id, msg := 
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		range node.knowData {
			
            fmt.Printf("   -> Msg ID: %s | Origin: %s | Value: %q\n", id[:8] + "...", msg.Origin, msg.Value)
        }
        node.mu.Unlock()
    }
}