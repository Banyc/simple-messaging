# Simple Messaging

Simple messaging for pub/sub and producer/consumer. Pure Go!

# Usage

## Request-Response

Producer:

```go
consumerAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
if err != nil {
    panic(err)
}
producer := messaging.NewProducer(consumerAddr)
producer.Start()
defer producer.Close()

producer.EnsureSent([]byte("Hello World!"))
```

Consumer:

```go
consumerListenAddr, err := net.ResolveTCPAddr("tcp", ":8080")
if err != nil {
    panic(err)
}
consumer := messaging.NewConsumer(consumerListenAddr, 1024)
consumer.Start()
defer consumer.Close()

message := consumer.Receive()
```

## Pub-Sub

Subscriber:

```go
pubAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
if err != nil {
    panic(err)
}
sub := messaging.NewSubscriber(pubAddr, 1024)
sub.Start()
defer sub.Close()

message := sub.EnsureReceived()
```

Publisher:

```go
pubListenAddr, err := net.ResolveTCPAddr("tcp", ":8080")
if err != nil {
    panic(err)
}
pub := messaging.NewPublisher(pubListenAddr)
pub.Start()
defer pub.Close()

pub.Send([]byte("Hello World!"))
```

# TODO

-   [ ]  Receivers confirm receipt of messages via reply
    -   make sure every message is delivered at least once
