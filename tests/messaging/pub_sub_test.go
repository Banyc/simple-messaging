package messaging_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/banyc/simple-messaging/messaging"
)

func TestPubSub1(t *testing.T) {
	pubListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()
	defer pub.Close()

	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	sub := messaging.NewSubscriber(pubAddr, 1024)
	sub.Start()
	defer sub.Close()

	time.Sleep(time.Second)

	pub.Send([]byte("Hello World!"))
	subRecvBytes := sub.EnsureReceived()
	if string(subRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(subRecvBytes) + "'")
	}
}

func TestPubSub1_1(t *testing.T) {
	pubListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()
	defer pub.Close()

	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	sub := messaging.NewSubscriber(pubAddr, 1)
	sub.Start()
	defer sub.Close()

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
	defer sub.Close()

	pubListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()
	defer pub.Close()

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
		defer sub.Close()
		subs = append(subs, sub)
	}

	pubListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()
	defer pub.Close()

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
		defer sub.Close()
		subs = append(subs, sub)
	}

	pubListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()
	defer pub.Close()

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
	pub.Send([]byte("abc"))
	pub.Send([]byte("def"))
	for _, sub := range subs {
		subRecvBytes := sub.EnsureReceived()
		if string(subRecvBytes) != "abc" {
			t.Fatal("Expected 'abc' but got '" + string(subRecvBytes) + "'")
		}
		subRecvBytes = sub.EnsureReceived()
		if string(subRecvBytes) != "def" {
			t.Fatal("Expected 'def' but got '" + string(subRecvBytes) + "'")
		}
	}
}

func TestPubSub5(t *testing.T) {
	pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	subs := make([]*messaging.Subscriber, 0)
	for i := 0; i < 10; i++ {
		sub := messaging.NewSubscriber(pubAddr, 1024)
		sub.Start()
		defer sub.Close()
		subs = append(subs, sub)
	}

	pubListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewPublisher(pubListenAddr)
	pub.Start()
	defer pub.Close()

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

	wg := sync.WaitGroup{}

	for _, sub := range subs {
		wg.Add(1)
		go func(sub *messaging.Subscriber) {
			subRecvBytes := sub.EnsureReceived()
			if string(subRecvBytes) != "abc" {
				t.Fatal("Expected 'abc' but got '" + string(subRecvBytes) + "'")
			}
			subRecvBytes = sub.EnsureReceived()
			if string(subRecvBytes) != "def" {
				t.Fatal("Expected 'def' but got '" + string(subRecvBytes) + "'")
			}
			wg.Done()
		}(sub)
	}

	// restart publisher
	pub.Close()
	pub = messaging.NewPublisher(pubListenAddr)
	pub.Start()
	time.Sleep(time.Second * 3)
	pub.Send([]byte("abc"))
	pub.Send([]byte("def"))

	wg.Wait()
}
