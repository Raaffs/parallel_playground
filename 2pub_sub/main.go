



























































































































































package main







































import (
	"fmt"
	"sync"
	"time"
)


type Broker struct{
	mutex sync.RWMutex
	subs  map[chan int]struct{}
}

func Init()*Broker{
	return &Broker{
		mutex: sync.RWMutex{},
		subs: map[chan int]struct{}{},
	}
}

func(b *Broker)Publish(data int){
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for sub:=range b.subs{
		select{
		case sub<-data:
		default:
		}
	}
}

func (b *Broker)Subscribe()chan int{
	b.mutex.Lock()
	defer b.mutex.Unlock()	
	ch:=make(chan int,10)
	b.subs[ch]=struct{}{}
	return ch
}

func (b *Broker)Unsubscribe(sub chan int){
	b.mutex.Lock()
	defer b.mutex.Unlock()

	close(sub)
	delete(b.subs,sub)
}


func main(){
	broker:=Init()

	var cWg sync.WaitGroup
	var pWg sync.WaitGroup

	for i:=range 5{
		cWg.Add(1)
		go func ()  {
			defer cWg.Done()
			channel:=broker.Subscribe()	
			for ch:=range channel{
				fmt.Printf("consumer %d got message : %d\n",i+1,ch)
			}
		}()
	}

	for i :=range 3{
		pWg.Add(1)
		go func ()  {
			defer pWg.Done()
			for j:=range 10{
				time.Sleep(40*time.Millisecond)
				broker.Publish((i+1)*10+j)
			}
		}()
	}

	go func ()  {
		pWg.Wait()
		broker.mutex.Lock()
		defer broker.mutex.Lock()
		for sub := range broker.subs{
			close(sub)
		}
	}()

	cWg.Wait()
}