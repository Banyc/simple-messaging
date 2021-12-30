package messaging

import "encoding/binary"

type Frame struct {
	message []byte
}

func RestoreFrame(frame []byte) *Frame {
	if len(frame) < 4 {
		return nil
	}
	messageSize := binary.BigEndian.Uint32(frame[:4])
	if len(frame)-4 < int(messageSize) {
		return nil
	}

	return &Frame{
		message: frame[4 : 4+messageSize],
	}
}

func NewFrame(message []byte) *Frame {
	return &Frame{
		message: message,
	}
}

func (this *Frame) GetBytes() []byte {
	messageSize := uint32(len(this.message))
	bytes := make([]byte, 4+messageSize)
	binary.BigEndian.PutUint32(bytes[:4], messageSize)
	copy(bytes[4:], this.message)
	return bytes
}

func (this *Frame) GetMessage() []byte {
	return this.message
}
