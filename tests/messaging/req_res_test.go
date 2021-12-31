package messaging_test

import (
	"net"
	"simple-messaging/messaging"
	"testing"
	"time"
)

func TestReqRes1(t *testing.T) {
	consumerListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8079")
	if err != nil {
		t.Fatal(err)
	}
	consumer := messaging.NewConsumer(consumerListenAddr, 1024)
	consumer.Start()
	defer consumer.Close()

	consumerAddr, err := net.ResolveTCPAddr("tcp", "localhost:8079")
	if err != nil {
		t.Fatal(err)
	}
	producer := messaging.NewProducer(consumerAddr)
	producer.Start()
	defer producer.Close()

	time.Sleep(time.Second)

	producer.EnsureSent([]byte("Hello World!"))
	consumerRecvBytes := consumer.Receive()
	if string(consumerRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(consumerRecvBytes) + "'")
	}
}

func TestReqRes1_1(t *testing.T) {
	consumerListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	consumer := messaging.NewConsumer(consumerListenAddr, 1)
	consumer.Start()
	defer consumer.Close()

	consumerAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	producer := messaging.NewProducer(consumerAddr)
	producer.Start()
	defer producer.Close()

	time.Sleep(time.Second)

	producer.EnsureSent([]byte("Hello World!"))
	consumerRecvBytes := consumer.Receive()
	if string(consumerRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(consumerRecvBytes) + "'")
	}
}
