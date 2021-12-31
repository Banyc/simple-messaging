package messaging_test

import (
	"net"
	"simple-messaging/messaging"
	"testing"
	"time"
)

func TestPubSub1(t *testing.T) {
	publistenAddr, err := net.ResolveTCPAddr("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(publistenAddr)
	pub.Start()

	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	sub := messaging.NewSubscriber(pubAddr, 1024)
	sub.Start()

	time.Sleep(time.Second)

	pub.Send([]byte("Hello World!"))
	subRecvBytes := sub.EnsureReceived()
	if string(subRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
	}
}

func TestPubSub2(t *testing.T) {
	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	sub := messaging.NewSubscriber(pubAddr, 1024)
	sub.Start()

	pubListenAddr, err := net.ResolveTCPAddr("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()

	time.Sleep(time.Second * 3)

	pub.Send([]byte("Hello World!"))
	subRecvBytes := sub.EnsureReceived()
	if string(subRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
	}
}

func TestPubSub3(t *testing.T) {
	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	subs := make([]*messaging.Subscriber, 0)
	for i := 0; i < 10; i++ {
		sub := messaging.NewSubscriber(pubAddr, 1024)
		sub.Start()
		subs = append(subs, sub)
	}

	pubListenAddr, err := net.ResolveTCPAddr("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()

	time.Sleep(time.Second * 3)

	pub.Send([]byte("Hello World!"))
	for _, sub := range subs {
		subRecvBytes := sub.EnsureReceived()
		if string(subRecvBytes) != "Hello World!" {
			t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
		}
	}
}

func TestPubSub4(t *testing.T) {
	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	subs := make([]*messaging.Subscriber, 0)
	for i := 0; i < 10; i++ {
		sub := messaging.NewSubscriber(pubAddr, 1024)
		sub.Start()
		subs = append(subs, sub)
	}

	pubListenAddr, err := net.ResolveTCPAddr("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()

	time.Sleep(time.Second * 3)

	pub.Send([]byte("Hello"))
	pub.Send([]byte("World!"))
	pub.Send([]byte("Hello World!"))
	for _, sub := range subs {
		subRecvBytes := sub.EnsureReceived()
		if string(subRecvBytes) != "Hello" {
			t.Fatal("Expected 'Hello' but got '" + string(subRecvBytes) + "'")
		}
		subRecvBytes = sub.EnsureReceived()
		if string(subRecvBytes) != "World!" {
			t.Fatal("Expected 'World!' but got '" + string(subRecvBytes) + "'")
		}
		subRecvBytes = sub.EnsureReceived()
		if string(subRecvBytes) != "Hello World!" {
			t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
		}
	}
}
