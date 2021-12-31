package messaging_test

import (
	"net"
	"simple-messaging/messaging"
	"testing"
	"time"
)

func TestPubSub1(t *testing.T) {
	pubAddr, err := net.ResolveTCPAddr("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubAddr)
	pub.Start()

	subAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	sub := messaging.NewSubscriber(subAddr, 1024)
	sub.Start()

	time.Sleep(time.Second)

	pub.Send([]byte("Hello World!"))
	subRecvBytes := sub.EnsureReceived()
	if string(subRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
	}
}

func TestPubSub2(t *testing.T) {
	subAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	sub := messaging.NewSubscriber(subAddr, 1024)
	sub.Start()

	pubAddr, err := net.ResolveTCPAddr("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubAddr)
	pub.Start()

	time.Sleep(time.Second * 3)

	pub.Send([]byte("Hello World!"))
	subRecvBytes := sub.EnsureReceived()
	if string(subRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
	}
}
