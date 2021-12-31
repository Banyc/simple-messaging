package messaging

import (
	"net"
	"simple-messaging/messaging/dto"
	"sync"
)

type Consumer struct {
	listener            *net.TCPListener
	rxBufferSize        int
	consumerWorkers     []*ConsumerWorker
	consumerWorkerMutex sync.RWMutex
	rxFrames            chan *dto.Frame
	isClosed            bool
}

func NewConsumer(
	listenAddress *net.TCPAddr,
	rxBufferSize int,
) *Consumer {
	listener, err := net.ListenTCP("tcp", listenAddress)
	if err != nil {
		panic(err)
	}
	this := &Consumer{
		listener:     listener,
		rxBufferSize: rxBufferSize,
		rxFrames:     make(chan *dto.Frame),
		isClosed:     false,
	}
	return this
}

func (this *Consumer) Start() {
	go func() {
		for {
			conn, err := this.listener.AcceptTCP()
			if err != nil {
				if this.isClosed {
					return
				}
				panic(err)
			}
			this.consumerWorkerMutex.Lock()
			consumerWorker := NewConsumerWorker(
				this,
				conn,
				this.rxBufferSize,
			)
			consumerWorker.Start()
			this.consumerWorkers = append(this.consumerWorkers, consumerWorker)
			this.consumerWorkerMutex.Unlock()
		}
	}()
}

func (this *Consumer) Close() {
	this.isClosed = true
	this.listener.Close()
	this.consumerWorkerMutex.Lock()
	for _, consumerWorker := range this.consumerWorkers {
		consumerWorker.Close()
	}
	this.consumerWorkers = make([]*ConsumerWorker, 0)
	this.consumerWorkerMutex.Unlock()
	close(this.rxFrames)
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

func (this *Consumer) ReceivedFrame(frame *dto.Frame) {
	if this.isClosed {
		return
	}
	this.rxFrames <- frame
}

func (this *Consumer) Receive() []byte {
	frame := <-this.rxFrames
	if frame == nil {
		return nil
	}
	return frame.GetMessage()
}
