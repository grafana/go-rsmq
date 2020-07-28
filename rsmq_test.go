package rsmq

import (
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

var client *redis.Client
var r *RedisSMQ

func cleanup(t *testing.T) {
	queues, err := r.ListQueues()
	if err != nil {
		t.Fatalf("could not list queues: %v", err)
	}

	for _, queueName := range queues {
		for {
			message, err := r.PopMessage(queueName)
			if err != nil {
				t.Fatalf("could not pop message: %v", err)
			}
			if message == nil {
				break
			}
		}

		err = r.DeleteQueue(queueName)
		if err != nil {
			t.Fatalf("could not delete queue: %v", err)
		}
	}
}

func TestMain(m *testing.M) {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	r = NewRedisSMQ(client, "rsmqtest")

	code := m.Run()

	cleanup(&testing.T{})

	os.Exit(code)
}

func TestCreateQueueWithDefaultAttributes(t *testing.T) {
	queueName := "test"

	err := r.CreateQueue(queueName)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	qa, err := r.GetQueueAttributes(queueName)
	if err != nil {
		t.Fatalf("could not get queue attributes: %v", err)
	}

	if qa.VisibiltyTimeout != 30 {
		t.Fatalf("visibility timeout should be %d not %d", 30, qa.VisibiltyTimeout)
	}

	if qa.Delay != 0 {
		t.Fatalf("delay should be %d not %d", 0, qa.Delay)
	}

	if qa.MaxSize != 65535 {
		t.Fatalf("demax sizelay should be %d not %d", 65535, qa.MaxSize)
	}

	cleanup(t)
}

func TestCreateQueueWithCustomAttributes(t *testing.T) {
	queueName := "test"

	err := r.CreateQueueWithAttributes(queueName, 100, 200, 2048)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	qa, err := r.GetQueueAttributes(queueName)
	if err != nil {
		t.Fatalf("could not get queue attributes: %v", err)
	}

	if qa.VisibiltyTimeout != 100 {
		t.Fatalf("visibility timeout should be %d not %d", 100, qa.VisibiltyTimeout)
	}

	if qa.Delay != 200 {
		t.Fatalf("delay should be %d not %d", 200, qa.Delay)
	}

	if qa.MaxSize != 2048 {
		t.Fatalf("demax sizelay should be %d not %d", 2048, qa.MaxSize)
	}

	cleanup(t)
}

func TestListQueues(t *testing.T) {
	queue1Name := "test1"
	queue2Name := "test2"
	queue3Name := "test3"

	err := r.CreateQueue(queue1Name)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	err = r.CreateQueue(queue2Name)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	err = r.CreateQueue(queue3Name)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	queues, err := r.ListQueues()
	if err != nil {
		t.Fatalf("could not list queues: %v", err)
	}

	if len(queues) != 3 {
		t.Fatalf("number of queues should be %d not %d", 3, len(queues))
	}

	hasQueue1 := false
	hasQueue2 := false
	hasQueue3 := false

	for _, v := range queues {
		if v == queue1Name {
			hasQueue1 = true
		}
		if v == queue2Name {
			hasQueue2 = true
		}
		if v == queue3Name {
			hasQueue3 = true
		}
	}

	if !hasQueue1 || !hasQueue2 || !hasQueue3 {
		t.Fatalf("all queues were not listed")
	}

	err = r.DeleteQueue(queue3Name)
	if err != nil {
		t.Fatalf("could not delete queue: %v", err)
	}

	queues, err = r.ListQueues()
	if err != nil {
		t.Fatalf("could not list queues: %v", err)
	}

	if len(queues) != 2 {
		t.Fatalf("number of queues should be %d not %d", 2, len(queues))
	}

	hasQueue1 = false
	hasQueue2 = false
	hasQueue3 = false

	for _, v := range queues {
		if v == queue1Name {
			hasQueue1 = true
		}
		if v == queue2Name {
			hasQueue2 = true
		}
		if v == queue3Name {
			hasQueue3 = true
		}
	}

	if !hasQueue1 || !hasQueue2 || hasQueue3 {
		t.Fatalf("all queues were not listed")
	}

	cleanup(t)
}

func TestSetQueueAttributes(t *testing.T) {
	queueName := "test"

	err := r.CreateQueue(queueName)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	_, err = r.SetQueueAttributes(queueName, 100, 200, 3000)
	if err != nil {
		t.Fatalf("could not set queue attributes: %v", err)
	}

	qa, err := r.GetQueueAttributes(queueName)
	if err != nil {
		t.Fatalf("could not get queue attributes: %v", err)
	}

	if qa.VisibiltyTimeout != 100 {
		t.Fatalf("visibility timeout should be %d not %d", 100, qa.VisibiltyTimeout)
	}

	if qa.Delay != 200 {
		t.Fatalf("delay should be %d not %d", 200, qa.Delay)
	}

	if qa.MaxSize != 3000 {
		t.Fatalf("demax sizelay should be %d not %d", 3000, qa.MaxSize)
	}

	cleanup(t)
}

func TestSendMessage(t *testing.T) {
	queueName := "test"
	message1 := "blah1"
	message2 := "blah2"

	err := r.CreateQueue(queueName)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	message1ID, err := r.SendMessage(queueName, message1)
	if err != nil {
		t.Fatalf("could not send message: %v", err)
	}

	message2ID, err := r.SendMessage(queueName, message2)
	if err != nil {
		t.Fatalf("could not send message: %v", err)
	}

	rx1, err := r.PopMessage(queueName)
	if err != nil {
		t.Fatalf("could not pop message: %v", err)
	}

	if rx1.ID != message1ID {
		t.Fatalf("message ID should be %s not %s", message1ID, rx1.ID)
	}

	if rx1.Message != message1 {
		t.Fatalf("message should be %s not %s", message1ID, rx1.ID)
	}

	rx2, err := r.PopMessage(queueName)
	if err != nil {
		t.Fatalf("could not pop message: %v", err)
	}

	if rx2.ID != message2ID {
		t.Fatalf("message ID should be %s not %s", message2ID, rx2.ID)
	}

	if rx2.Message != message2 {
		t.Fatalf("message should be %s not %s", message2ID, rx2.ID)
	}

	cleanup(t)
}

func TestSendMessageWithDelay(t *testing.T) {
	queueName := "test"
	message := "blah"

	err := r.CreateQueue(queueName)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	_, err = r.SendMessageWithDelay(queueName, message, 1)
	if err != nil {
		t.Fatalf("could not send message: %v", err)
	}

	rx, err := r.PopMessage(queueName)
	if err != nil {
		t.Fatalf("could not pop message: %v", err)
	}

	if rx != nil {
		t.Fatalf("received message before delay")
	}

	time.Sleep(time.Second)

	rx, err = r.PopMessage(queueName)
	if err != nil {
		t.Fatalf("could not pop message: %v", err)
	}

	if rx == nil {
		t.Fatalf("did not receive message after delay")
	}

	cleanup(t)
}

func TestReceiveMessageWithTimeout(t *testing.T) {
	queueName := "test"
	message1 := "blah1"
	message2 := "blah2"

	err := r.CreateQueue(queueName)
	if err != nil {
		t.Fatalf("could not create queue: %v", err)
	}

	_, err = r.SendMessage(queueName, message1)
	if err != nil {
		t.Fatalf("could not send message: %v", err)
	}

	_, err = r.SendMessage(queueName, message2)
	if err != nil {
		t.Fatalf("could not send message: %v", err)
	}

	rx, err := r.ReceiveMessageWithTimeout(queueName, 2)
	if err != nil {
		t.Fatalf("could not receive message: %v", err)
	}

	if rx.Message != message1 {
		t.Fatalf("received message before delay")
	}

	rx, err = r.ReceiveMessageWithTimeout(queueName, 1)
	if err != nil {
		t.Fatalf("could not receive message: %v", err)
	}

	if rx.Message != message2 {
		t.Fatalf("received message before delay")
	}

	rx, err = r.ReceiveMessageWithTimeout(queueName, 1)
	if err != nil {
		t.Fatalf("could not receive message: %v", err)
	}

	if rx != nil {
		t.Fatalf("all message should be invisible")
	}

	time.Sleep(time.Second + 10*time.Millisecond)
	rx, err = r.ReceiveMessageWithTimeout(queueName, 2)
	if err != nil {
		t.Fatalf("could not receive message: %v", err)
	}

	if rx.Message != message2 {
		t.Fatalf("received message before delay")
	}

	time.Sleep(time.Second + 10*time.Millisecond)
	rx, err = r.ReceiveMessageWithTimeout(queueName, 1)
	if err != nil {
		t.Fatalf("could not receive message: %v", err)
	}

	if rx.Message != message1 {
		t.Fatalf("received message before delay")
	}

	time.Sleep(time.Second + 10*time.Millisecond)
	rx, err = r.PopMessage(queueName)
	if err != nil {
		t.Fatalf("could not pop message: %v", err)
	}

	if rx.Message != message2 {
		t.Fatalf("received message before delay")
	}

	rx, err = r.PopMessage(queueName)
	if err != nil {
		t.Fatalf("could not pop message: %v", err)
	}

	if rx.Message != message1 {
		t.Fatalf("received message before delay")
	}

	att, err :=  r.GetQueueAttributes(queueName)
	if err != nil {
		t.Fatalf("could not get queue attributes")
	}
	if att.Messages != 0 {
		t.Fatalf("queue should be empty")
	}
	cleanup(t)
}
