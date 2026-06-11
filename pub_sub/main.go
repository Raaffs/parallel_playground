package main

import (
	"fmt"
	"log"
	// "log"
	"sync"
)

type Broker struct{
	subChan chan chan int 
	pubChan chan int 
}

func NewBroker()*Broker{
	b:=&Broker{
		subChan: make(chan chan int),
		pubChan: make(chan int),
	}
	go b.serve()
	return b
}

type BackLog map[chan int][]int
func(b *Broker)serve(){
	activeSubs:=make(map[chan int]struct{})
	backlog:=make(BackLog)

	for{
		select{
		case sub:=<-b.subChan:
			activeSubs[sub]=struct{}{}
		case data,ok:=<-b.pubChan:
			if !ok{
				for sub:=range activeSubs{
					if remaining,exist:=backlog[sub];exist{
						for d:=range remaining{
							sub<-d
						}
						close(sub)
					}else{
						close(sub)
					}
				}
				return 
			}
			for sub:=range activeSubs{
				b.broadcast(data, sub, backlog)
			}
		}
	}
}

func(b *Broker)broadcast(data int, sub chan int, backlog BackLog){
	if len(backlog[sub])>0{
		backlog[sub]=append(backlog[sub], data)
		b.drainBacklog(sub,backlog)
		return 
	}
	select{
	case sub<-data:
	default:
		backlog[sub]=[]int{data}
	}
}

func(b *Broker)drainBacklog(sub chan int, backlog BackLog){
	queue:=backlog[sub]
	for len(queue)>0{
		select{
		case sub<-queue[0]:
			queue=queue[1:]
		default:
			backlog[sub]=queue
			return
		}
	}
	delete(backlog,sub)
}
func(b *Broker)Publish(data int) {b.pubChan<-data}
func(b *Broker)Subscribe(sub chan int) {b.subChan<-sub}

func main(){
	broker:=NewBroker()

	var cWg sync.WaitGroup
	var pWg sync.WaitGroup

	for i:=range 5{
		cWg.Add(1)
		msgs:=[]int{}
		channel:=make(chan int,10)
		broker.Subscribe(channel)
		go func ()  {
			defer cWg.Done()
			for msg:=range channel{
				msgs = append(msgs, msg)
				fmt.Printf("consumer %d received msg %d\n",i+1,msg)
			}
			if len(msgs)!=30{
				log.Fatalf("consumer %d received %d msgs, expected %d\n",(i+1),len(msgs),30)
			}else{
				fmt.Printf("consumer %d received all msgs\n",(i+1))
			}
		}()
	}

	for i:=range 3{
		pWg.Add(1)
		go func ()  {
			defer pWg.Done()
			for j:=range 10{
				broker.Publish((i+1)*10+j)
			}
		}()
	}

	go func ()  {
		pWg.Wait()
		close(broker.pubChan)
	}()
	cWg.Wait()
}