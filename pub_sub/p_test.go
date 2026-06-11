package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestParallel(t *testing.T) {
	broker := NewBroker()
	var cWG sync.WaitGroup
	var pWG sync.WaitGroup

	// FIX 1: Properly capture 'i' by passing it into the goroutine
	for i := range 5 {
		cWG.Add(1)
		channel := make(chan int, 10)
		broker.Subscribe(channel)
		
		go func(id int) { 
			msgs:=[]int{}
			defer cWG.Done()
			for msg := range channel {
				fmt.Printf("subscribe %d got msg %d\n", id, msg)
				msgs=append(msgs, msg)
			}
			if len(msgs)!=30{
				t.Fatalf("expected 30 got %d",len(msgs))
			}
		}(i + 1) // Pass current value here
	}

	for i := range 3 {
		pWG.Add(1)
		go func(pID int) {
			defer pWG.Done()
			for j := range 10 {
				time.Sleep(60*time.Millisecond)
				broker.Publish(pID*10 + j)
			}
		}(i + 1)
	}

	go func() {
		pWG.Wait()
		close(broker.pubChan)
	}()

	cWG.Wait()

}