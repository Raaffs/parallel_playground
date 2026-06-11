package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"
)

type Output struct{}
func producer(buff chan int){
	for i:=range 10 {
		n,_:=rand.Int(rand.Reader,big.NewInt(700))
		time.Sleep(time.Duration(n.Int64())*time.Millisecond)
		buff<-i+1
	}
}

func consumer(buff chan int){
	for val:=range buff{
		fmt.Println(val)
	}
}

func main(){
	buff:=make(chan int,10)
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	producerWg.Add(2)
	go func () {
		defer producerWg.Done()
		producer(buff)
	}()

	go func () {
		defer producerWg.Done()
		producer(buff)
	}()

	go func ()  {
		defer consumerWg.Done()
		consumer(buff)
	}()

	producerWg.Wait()
	close(buff)
	consumerWg.Wait()

}