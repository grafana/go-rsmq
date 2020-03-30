# go-rsmq

[![GoDoc](https://godoc.org/github.com/grafana/go-rsmq?status.svg)](https://godoc.org/github.com/grafana/go-rsmq)

A Go implementation of the Node.js [rsmq](https://github.com/smrchy/rsmq) package.

## Example

```go
// Create a redis client
client := redis.NewClient(&redis.Options{
  Addr:     "localhost:6379",
})

mq := rsmq.NewRedisSMQ(client, "")

err = mq.CreateQueue("myqueue")
if err != nil {
  return err
}

messageID, err := mq.SendMessage("myqueue", "hello")
if err != nil {
  return err
}

message, err := mq.ReceiveMessage("myqueue")
if err != nil {
  return err
}

if message != nil {
  mq.DeleteMessage("myqueue", message.ID)
  if err != nil {
    return err
  }
}

```

## TODO
- Finish tests. The message send side is covered ok, but not the receive side.