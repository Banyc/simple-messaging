package messaging

import (
	"net"
	"simple-messaging/messaging/dto"
	"time"
)

type Producer struct {
	consumerAddress *net.TCPAddr
	consumer        *net.TCPConn
	txMessages      chan []byte
	isClosed        bool
}

func NewProducer(consumerAddress *net.TCPAddr) *Producer {
	this := &Producer{
		consumerAddress: consumerAddress,
		txMessages:      make(chan []byte),
		isClosed:        false,
	}
	return this
}

func (this *Producer) Start() {
	go this.ensureReconnected()
	go this.flushTXMessages()
}

func (this *Producer) Close() {
	this.isClosed = true
	if this.consumer != nil {
		this.consumer.Close()
	}
	close(this.txMessages)
}

// return: is successful
func (this *Producer) ensureReconnected() bool {
	if this.consumer != nil {
		this.consumer.Close()
	}
	isSucceeded := this.connect()
	for !isSucceeded {
		if this.isClosed {
			return false
		}
		time.Sleep(time.Second)
		isSucceeded = this.connect()
	}
	return true
}

// return: is successful
func (this *Producer) connect() bool {
	if this.isClosed {
		return false
	}
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
	for this.consumer == nil {
		// wait for consumer to be connected
		time.Sleep(time.Second)
	}
	frame := dto.NewFrame(message)
	frameBytes := frame.GetBytes()
	_, err := this.consumer.Write(frameBytes)
	if err != nil {
		return err
	}
	return nil
}

func (this *Producer) SendAsync(message []byte) {
	if this.isClosed {
		return
	}
	this.txMessages <- message
}

// return: is successful
func (this *Producer) EnsureSent(message []byte) bool {
	for {
		err := this.Send(message)
		if err != nil {
			ok := this.ensureReconnected()
			if !ok {
				// producer is closed
				return false
			}
			continue
		}
		return true
	}
}

func (this *Producer) flushTXMessages() {
	for {
		message := <-this.txMessages
		if this.isClosed {
			return
		}
		ok := this.EnsureSent(message)
		if !ok {
			// producer is closed
			return
		}
	}
}
