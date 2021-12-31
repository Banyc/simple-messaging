package messaging

import (
	"net"
)

type ConsumerWorker struct {
	consumer     *Consumer
	producer     *net.TCPConn
	rxBuffer     []byte
	rxBytesSoFar []byte
}

func NewConsumerWorker(
	consumer *Consumer,
	producer *net.TCPConn,
	rxBufferSize int,
) *ConsumerWorker {
	this := &ConsumerWorker{
		consumer: consumer,
		producer: producer,
		rxBuffer: make([]byte, rxBufferSize),
	}
	return this
}

func (this *ConsumerWorker) Start() {
	go func() {
		for {
			frame, err := this.receive()
			if err != nil {
				this.consumer.ProducerClosed(this)
				return
			}
			this.consumer.ReceivedFrame(frame)
		}
	}()
}

// notice: the returned slice will not be reused in the next call
func (this *ConsumerWorker) receive() (*Frame, error) {
	readByteCount := 0
	frame := this.restoreFrameFromRXBytesSoFar()
	if frame != nil {
		return frame, nil
	}
	nr, err := this.producer.Read(this.rxBuffer)
	readByteCount = nr
	if err != nil {
		return nil, err
	}
	// it will always perform a deep copy in this case
	this.rxBytesSoFar = append(this.rxBytesSoFar, this.rxBuffer[:readByteCount]...)
	frame = this.restoreFrameFromRXBytesSoFar()
	if frame == nil {
		return nil, nil
	}
	return frame, nil
}

func (this *ConsumerWorker) restoreFrameFromRXBytesSoFar() *Frame {
	frameSize, frame := RestoreFrame(this.rxBytesSoFar)
	if frame == nil {
		return nil
	}
	this.rxBytesSoFar = this.rxBytesSoFar[frameSize:]
	return frame
}
