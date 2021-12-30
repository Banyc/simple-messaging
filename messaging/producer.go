package messaging

import (
	"net"
	"time"
)

type Producer struct {
	consumerAddress *net.TCPAddr
	consumer        *net.TCPConn
}

func NewProducer(consumerAddress *net.TCPAddr) *Producer {
	this := &Producer{
		consumerAddress: consumerAddress,
	}
	return this
}

func (this *Producer) Start() {
	go this.ensureReconnected()
}

func (this *Producer) ensureReconnected() {
	if this.consumer != nil {
		this.consumer.Close()
	}
	isSucceeded := this.connect()
	for !isSucceeded {
		time.Sleep(time.Second)
		isSucceeded = this.connect()
	}
}

// return: is successful
func (this *Producer) connect() bool {
	conn, err := net.DialTCP("tcp", nil, this.consumerAddress)
	if err != nil {
		return false
	}
	err = conn.SetKeepAlive(true)
	if err != nil {
		panic(err)
	}
	this.consumer = conn
	return true
}

func (this *Producer) Send(message []byte) error {
	if this.consumer == nil {
		// wait for consumer to be connected
		time.Sleep(time.Second)
	}
	frame := NewFrame(message)
	frameBytes := frame.GetBytes()
	_, err := this.consumer.Write(frameBytes)
	if err != nil {
		return err
	}
	return nil
}

func (this *Producer) EnsureSent(message []byte) {
	for {
		err := this.Send(message)
		if err != nil {
			this.ensureReconnected()
			continue
		}
		return
	}
}
