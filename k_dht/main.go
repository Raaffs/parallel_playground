package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"sync"
)

type ID [20]byte
type PeerBucket [160][]ID 

type MessageType int 
const (
	//Tells a target peer to store a file hash and its data in its local data map.
	STORE_RES MessageType=iota
	STORE_REQ 

	//Requests or responds with the file data (or the addresses of peers holding the file) 
	// associated with a specific file hash
	FIND_VALUE_REQ
	FIND_VALUE_RES

	//Requests the k closest nodes to a target ID.
	
)

type Request struct{
	Type MessageType 
	File File
}

type File struct {
	ID 			ID
	Origin		string
	ForwardedBy ID
	Content		string
}
type Node struct{
	ID 				ID
	addr 			string
	AvailableData	map[[20]byte]string
	listener		net.Listener
	mu 				sync.Mutex
	peers			PeerBucket
}

func NewNode(id ID, addr string, peers PeerBucket)*Node{
	n:=&Node{
		ID: id,
		addr: addr,
		peers: peers,
		mu: sync.Mutex{},
		AvailableData: make(map[[20]byte]string),
	}

	go n.Start()

	return n 
}

func(n *Node)Start(){
	var err error
	n.listener,err=net.Listen("tcp",n.addr); if err!=nil{
		fmt.Printf("Node %s failed to start : %v\n", n.ID,err)
		return
	}

	for{
		conn,err:=n.listener.Accept(); if err!=nil{
			fmt.Printf("Node %s had error accepting connection %v\n",n.ID,err)
		}else{
			go n.handleIncomingConnection(conn)
		}
		
	}
}

func(n *Node)handleIncomingConnection(conn net.Conn){
	var req Request
	defer conn.Close()

	if err:=json.NewDecoder(conn).Decode(&req);err!=nil{
		fmt.Printf("Node %s unable to decode connection json : %v\n",n.ID,err)
		return
	}
}

func(n *Node)FindFileOrClosestPeer(file File){
	n.mu.Lock()
	defer n.mu.Unlock()
	if f,exist:=n.AvailableData[file.ID];exist{
		conn,err:=net.Dial("tcp",file.Origin); if err!=nil{
			fmt.Printf("Node %s had error while opening connection to node %s while sending back the file : %v",n.ID,file.Origin,err)
			return
		}
		json.NewEncoder(conn).Encode(f)
	}
}

func(n *Node)FillBucket(peers []ID){
	for _,peer:=range peers{
		distance:=n.getBucketIndex(peer); if distance!=-1{
			n.peers[distance]=append(n.peers[distance], peer)
		}
	}
}

func(n *Node)getBucketIndex(peer ID)int{
	leadingZeroes:=0

	for i:=range 20{
		xorDistance:=n.ID[i]^peer[i]

		if xorDistance==byte(0){
			leadingZeroes+=8
			continue
		}

		for mask:=byte(128);mask>0;mask>>=1{
			if xorDistance & mask == 0 {
				leadingZeroes++
			}else{
				break
			}
		}
		// Invert the index so larger distances get larger bucket indices
        return 159 - leadingZeroes
	}
	return -1
}

func(n *Node)nonEmptyBucketIndexes()[]int{
	var nonEmptyBuckets []int
	for i,bucket:=range n.peers{
		if len(bucket)!=0{
			nonEmptyBuckets = append(nonEmptyBuckets, i)
		}
	}
	return nonEmptyBuckets
}

func (n *Node)findClosestBucket(filehash ID)int{
	fileBucketIndex:=n.getBucketIndex(filehash)
	if len(n.peers[fileBucketIndex])!=0{
		return fileBucketIndex
	}
	nonEmptyBuckets:=n.nonEmptyBucketIndexes()
	size:=len(nonEmptyBuckets)
	if size == 0 {
		return -1 
	}

	if fileBucketIndex<=nonEmptyBuckets[0]{
		return nonEmptyBuckets[0]
	}

	if fileBucketIndex>=nonEmptyBuckets[size-1]{
		return nonEmptyBuckets[size-1]
	}

	left,right:=0,size-1

	for left<right{
		mid:= left + (right-left)/2

		if fileBucketIndex<nonEmptyBuckets[mid]{
			right=mid
		}else{
			left=mid+1
		}
	}
	if (fileBucketIndex-nonEmptyBuckets[right])<(nonEmptyBuckets[left]-fileBucketIndex){
        return nonEmptyBuckets[right]
    } 
	return nonEmptyBuckets[left]
}



func(n *Node)FindClosestPeerInBucket(index int, file ID)ID{
	peers:=n.peers[index]
	slices.SortFunc(peers,func(a,b ID) int {
		var distA,distB [20]byte
		for i:=range 20{
			distA[i]=a[i]^file[i]
			distB[i]=b[i]^file[i]
		}
		return bytes.Compare(distA[:],distB[:])
	})
	//actual k-dht returns 20 closest peers but for this code
	//we're only returning the 1st one
	return peers[0]
}

