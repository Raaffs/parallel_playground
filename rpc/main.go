package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

type Respone struct{
	ID  int 
	Err error
}
func randomTimer()error{
	n,err:=rand.Int(rand.Reader,big.NewInt(10)); if err!=nil{
		return fmt.Errorf("random timer initialization failed: %v",err) 
	}
	num:=n.Int64()
	time.Sleep(time.Duration(num)*time.Millisecond)
	if num>=4{
		return fmt.Errorf("time out")
	}
	return nil 
}

func rpc1()Respone{return Respone{ID: 1,Err: randomTimer()}}
func rpc2()Respone{return Respone{ID: 2,Err: randomTimer()}}
func rpc3()Respone{return Respone{ID: 3,Err: randomTimer()}}
func rpc4()Respone{return Respone{ID: 4,Err: randomTimer()}}

func main(){
	for{
	rpcs:=[]func()Respone{rpc1,rpc2,rpc3,rpc4}
	maxRetries:=4
	resultChan:=make(chan Respone,len(rpcs))

	for _,rpc:=range rpcs{
		go func ()  {
			for i := range maxRetries{
				r:=rpc()
				if r.Err==nil{
					resultChan<-r 
					return
				}
				fmt.Printf("Attempt %d failed for RPC %d: %v\n", i+1, r.ID, r.Err)
			}
		}()
	}

	select{
	case r:=<-resultChan :
		fmt.Printf("rpc with ID %d responded\n",r.ID)
	case <-time.After(100*time.Millisecond):
		fmt.Println("All rpcs timed out")
	}
	time.Sleep(500*time.Millisecond)
}
}
