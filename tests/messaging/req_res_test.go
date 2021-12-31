package messaging_test

import (
	"net"
	"simple-messaging/messaging"
	"testing"
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

	ok := producer.EnsureSent([]byte("Hello World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
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

	ok := producer.EnsureSent([]byte("Hello World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
	consumerRecvBytes := consumer.Receive()
	if string(consumerRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(consumerRecvBytes) + "'")
	}
}

func TestReqRes2(t *testing.T) {
	consumerAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	producer := messaging.NewProducer(consumerAddr)
	producer.Start()
	defer producer.Close()

	consumerListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	consumer := messaging.NewConsumer(consumerListenAddr, 1)
	consumer.Start()
	defer consumer.Close()

	ok := producer.EnsureSent([]byte("Hello World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
	consumerRecvBytes := consumer.Receive()
	if string(consumerRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(consumerRecvBytes) + "'")
	}
}

func TestReqRes3(t *testing.T) {
	consumerAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	producer := messaging.NewProducer(consumerAddr)
	producer.Start()
	defer producer.Close()

	consumerListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	consumer := messaging.NewConsumer(consumerListenAddr, 1)
	consumer.Start()
	defer consumer.Close()

	ok := producer.EnsureSent([]byte("Hello"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
	ok = producer.EnsureSent([]byte("World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
	ok = producer.EnsureSent([]byte("Hello World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}

	consumerRecvBytes := consumer.Receive()
	if string(consumerRecvBytes) != "Hello" {
		t.Fatal("Expected 'Hello' but got '" + string(consumerRecvBytes) + "'")
	}
	consumerRecvBytes = consumer.Receive()
	if string(consumerRecvBytes) != "World!" {
		t.Fatal("Expected 'World!' but got '" + string(consumerRecvBytes) + "'")
	}
	consumerRecvBytes = consumer.Receive()
	if string(consumerRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(consumerRecvBytes) + "'")
	}
}

func TestReqRes4(t *testing.T) {
	consumerAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	producer := messaging.NewProducer(consumerAddr)
	producer.Start()
	defer producer.Close()

	consumerListenAddr, err := net.ResolveTCPAddr("tcp", "localhost:8078")
	if err != nil {
		t.Fatal(err)
	}
	consumer := messaging.NewConsumer(consumerListenAddr, 1)
	consumer.Start()
	defer consumer.Close()

	ok := producer.EnsureSent([]byte("Hello"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
	ok = producer.EnsureSent([]byte("World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}
	ok = producer.EnsureSent([]byte("Hello World!"))
	if !ok {
		t.Fatal("Unexpectedly closed")
	}

	consumerRecvBytes := consumer.Receive()
	if string(consumerRecvBytes) != "Hello" {
		t.Fatal("Expected 'Hello' but got '" + string(consumerRecvBytes) + "'")
	}
	consumerRecvBytes = consumer.Receive()
	if string(consumerRecvBytes) != "World!" {
		t.Fatal("Expected 'World!' but got '" + string(consumerRecvBytes) + "'")
	}
	consumerRecvBytes = consumer.Receive()
	if string(consumerRecvBytes) != "Hello World!" {
		t.Fatal("Expected 'Hello World!' but got '" + string(consumerRecvBytes) + "'")
	}

	// restart consumer
	consumer.Close()
	go func() {
		producer.EnsureSent([]byte("abc"))
		producer.EnsureSent([]byte("def"))
	}()
	consumer = messaging.NewConsumer(consumerListenAddr, 1)
	consumer.Start()

	consumerRecvBytes = consumer.Receive()
	if string(consumerRecvBytes) != "abc" {
		t.Fatal("Expected 'abc' but got '" + string(consumerRecvBytes) + "'")
	}
	consumerRecvBytes = consumer.Receive()
	if string(consumerRecvBytes) != "def" {
		t.Fatal("Expected 'def' but got '" + string(consumerRecvBytes) + "'")
	}
}
