package messaging

import (
	"net"
	"sync"
)

type Consumer struct {
	listener            *net.TCPListener
	consumerWorkers     []*ConsumerWorker
	consumerWorkerMutex sync.RWMutex

	rxFrames     chan *Frame
	rxFrameMutex sync.RWMutex
}

func NewConsumer(listenAddress *net.TCPAddr) *Consumer {
	listener, err := net.ListenTCP("tcp", listenAddress)
	if err != nil {
		panic(err)
	}
	this := &Consumer{
		listener: listener,
		rxFrames: make(chan *Frame),
	}
	return this
}

func (this *Consumer) Start() {
	go func() {
		for {
			conn, err := this.listener.AcceptTCP()
			if err != nil {
				panic(err)
			}
			this.consumerWorkerMutex.Lock()
			this.consumerWorkers = append(this.consumerWorkers,
				NewConsumerWorker(
					this,
					conn,
					1024,
				))
			this.consumerWorkerMutex.Unlock()
		}
	}()
}

func (this *Consumer) ProducerClosed(consumerWorker *ConsumerWorker) {
	this.consumerWorkerMutex.Lock()
	for i, cw := range this.consumerWorkers {
		if cw == consumerWorker {
			this.consumerWorkers = append(this.consumerWorkers[:i], this.consumerWorkers[i+1:]...)
			break
		}
	}
	this.consumerWorkerMutex.Unlock()
}

func (this *Consumer) ReceivedFrame(frame *Frame) {
	this.rxFrameMutex.Lock()
	this.rxFrames <- frame
	this.rxFrameMutex.Unlock()
}

func (this *Consumer) Receive() []byte {
	this.rxFrameMutex.Lock()
	defer this.rxFrameMutex.Unlock()
	frame := <-this.rxFrames
	return frame.GetMessage()
}
