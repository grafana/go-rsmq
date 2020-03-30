package rsmq

import (
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

// RedisSMQ is used to manage queues and messages
type RedisSMQ struct {
	client                        *redis.Client
	namespace                     string
	changeMessageVisibilityScript *redis.Script
	receiveMessageScript          *redis.Script
	popMessageScript              *redis.Script
}

// QueueAttributes contains settings and stats for a queue
type QueueAttributes struct {
	VisibiltyTimeout uint64
	Delay            uint64
	MaxSize          int64
	TotalReceived    uint64
	TotalSent        uint64
	CreatedAt        time.Time
	ModifiedAt       time.Time
	Messages         uint64
	HiddenMessages   uint64
}

// QueueMessage contains information for a message stored in a queue
type QueueMessage struct {
	ID              string
	Message         string
	SentAt          time.Time
	FirstReceivedAt time.Time
	ReceivedCount   uint64
}

// queueDesciptor is used internally to hold queue information
type queueDescriptor struct {
	VisibiltyTimeout uint64
	Delay            uint64
	MaxSize          int64
	TimestampMS      uint64
	UniqueID         string
}

// Errors that can be returned from RedisSMQ operations
var (
	ErrInvalidQueueName         = errors.New("invalid queue name")
	ErrInvalidMessageID         = errors.New("invalid message ID")
	ErrInvalidVisibilityTimeout = errors.New("visibility timeout must be between 0 and 9999999")
	ErrInvalidDelay             = errors.New("delay must be between 0 and 9999999")
	ErrInvalidMaxSize           = errors.New("max size must be between 1024 and 65535")
	ErrMessageTooLong           = errors.New("message too long")
	ErrQueueNotFound            = errors.New("queue not found")
	ErrQueueExists              = errors.New("queue exists")
	ErrMessageNotFound          = errors.New("message not found")
)

// NewRedisSMQ creates a new RedisSMQ object. The given namespace is prefixed to
// any Redis keys created by RSMQ. Defaults to "rsmq".
func NewRedisSMQ(client *redis.Client, namespace string) *RedisSMQ {
	if len(namespace) == 0 {
		namespace = "rsmq"
	}

	r := &RedisSMQ{
		client:    client,
		namespace: namespace + ":",
	}

	// Load lua scripts
	r.changeMessageVisibilityScript = redis.NewScript(changeMessageVisibilityScript)
	r.receiveMessageScript = redis.NewScript(receiveMessageScript)
	r.popMessageScript = redis.NewScript(popMessageScript)

	return r
}

// getQueue fetches the given queue info from redis and returns a QueueDescriptior. If
// uid is set to true, a new unique id will be generated and returned.
func (r *RedisSMQ) getQueue(queueName string, uid bool) (*queueDescriptor, error) {
	tx := r.client.TxPipeline()
	hmGetCmd := tx.HMGet(r.namespace+queueName+":Q", "vt", "delay", "maxsize")
	timeCmd := tx.Time()
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	hmGetValues := hmGetCmd.Val()
	if hmGetValues[0] == nil || hmGetValues[1] == nil || hmGetValues[2] == nil {
		return nil, ErrQueueNotFound
	}

	vt, err := toUint64(hmGetValues[0])
	if err != nil {
		return nil, errors.New("invalid visibility timeout retrieved")
	}
	delay, err := toUint64(hmGetValues[1])
	if err != nil {
		return nil, errors.New("invalid delay retrieved")
	}
	maxSize, err := toInt64(hmGetValues[2])
	if err != nil {
		return nil, errors.New("invalid max size retrieved")
	}

	ts := timeCmd.Val()
	timeUS := ts.UnixNano() / 1000
	timeMS := uint64(timeUS / 1000)

	quid := ""
	if uid {
		random, err := makeID(22)
		if err != nil {
			return nil, err
		}
		quid = strconv.FormatInt(timeUS, 36) + random
	}

	return &queueDescriptor{
		VisibiltyTimeout: vt,
		Delay:            delay,
		MaxSize:          maxSize,
		TimestampMS:      timeMS,
		UniqueID:         quid,
	}, nil
}

// CreateQueue makes a new queue with the given name. Sets the following options to their defaults:
// visibilityTimeout = 30, delay = 0, maxSize = 65535
func (r *RedisSMQ) CreateQueue(queueName string) error {
	return r.CreateQueueWithAttributes(queueName, 30, 0, 65535)
}

// CreateQueueWithAttributes creates a new queue using the given name and options
func (r *RedisSMQ) CreateQueueWithAttributes(queueName string, visibilityTimeout uint64, delay uint64, maxSize int64) error {

	if err := validateQueueName(queueName); err != nil {
		return err
	}
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		return err
	}
	if err := validateDelay(delay); err != nil {
		return err
	}
	if err := validateMaxSize(maxSize); err != nil {
		return err
	}

	key := r.namespace + queueName + ":Q"

	ts, err := r.client.Time().Result()
	if err != nil {
		return err
	}

	tx := r.client.TxPipeline()
	vtCmd := tx.HSetNX(key, "vt", visibilityTimeout)
	tx.HSetNX(key, "delay", delay)
	tx.HSetNX(key, "maxsize", maxSize)
	tx.HSetNX(key, "created", ts.Unix())
	tx.HSetNX(key, "modified", ts.Unix())
	if _, err := tx.Exec(); err != nil {
		return err
	}

	if !vtCmd.Val() {
		return ErrQueueExists
	}

	_, err = r.client.SAdd(r.namespace+"QUEUES", queueName).Result()
	return err
}

// ListQueues returns the names of all queues managed by rsmq
func (r *RedisSMQ) ListQueues() ([]string, error) {
	return r.client.SMembers(r.namespace + "QUEUES").Result()
}

// DeleteQueue deletes a queue with the given name from Redis
func (r *RedisSMQ) DeleteQueue(queueName string) error {
	if err := validateQueueName(queueName); err != nil {
		return err
	}

	key := r.namespace + queueName

	tx := r.client.TxPipeline()
	delCmd := tx.Del(key + ":Q")
	tx.SRem(r.namespace+"QUEUES", queueName)
	if _, err := tx.Exec(); err != nil {
		return err
	}

	if delCmd.Val() == 0 {
		return ErrQueueNotFound
	}

	return nil
}

// GetQueueAttributes returns stats and settings for the give queue
func (r *RedisSMQ) GetQueueAttributes(queueName string) (*QueueAttributes, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	key := r.namespace + queueName

	ts, err := r.client.Time().Result()
	if err != nil {
		return nil, err
	}

	tx := r.client.TxPipeline()
	hmGetCmd := tx.HMGet(key+":Q", "vt", "delay", "maxsize", "totalrecv", "totalsent", "created", "modified")
	zCardCmd := tx.ZCard(key)
	zCountCmd := tx.ZCount(key, strconv.FormatInt(ts.Unix(), 10)+"000", "+inf")
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	hmGetValues := hmGetCmd.Val()

	vt, err := toUint64(hmGetValues[0])
	if err != nil {
		return nil, errors.New("invalid visibility timeout retrieved")
	}
	delay, err := toUint64(hmGetValues[1])
	if err != nil {
		return nil, errors.New("invalid delay retrieved")
	}
	maxSize, err := toInt64(hmGetValues[2])
	if err != nil {
		return nil, errors.New("invalid max size retrieved")
	}
	totalReceived, err := toUint64(hmGetValues[3])
	if err != nil {
		totalReceived = 0
	}
	totalSent, err := toUint64(hmGetValues[4])
	if err != nil {
		totalReceived = 0
	}
	createdSec, err := toInt64(hmGetValues[5])
	if err != nil {
		return nil, errors.New("invalid created at time retrieved")
	}
	modifiedSec, err := toInt64(hmGetValues[6])
	if err != nil {
		return nil, errors.New("invalid modified at time retrieved")
	}

	return &QueueAttributes{
		VisibiltyTimeout: vt,
		Delay:            delay,
		MaxSize:          maxSize,
		TotalReceived:    totalReceived,
		TotalSent:        totalSent,
		CreatedAt:        time.Unix(createdSec, 0),
		ModifiedAt:       time.Unix(modifiedSec, 0),
		Messages:         uint64(zCardCmd.Val()),
		HiddenMessages:   uint64(zCountCmd.Val()),
	}, nil
}

func (r *RedisSMQ) SetQueueAttributes(queueName string, visibilityTimeout uint64, delay uint64, maxSize int64) (*QueueAttributes, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		return nil, err
	}
	if err := validateDelay(delay); err != nil {
		return nil, err
	}
	if err := validateMaxSize(maxSize); err != nil {
		return nil, err
	}

	qd, err := r.getQueue(queueName, false)
	if err != nil {
		return nil, err
	}

	key := r.namespace + queueName + ":Q"

	tx := r.client.TxPipeline()
	tx.HSet(key, "modified", qd.TimestampMS/1000)
	tx.HSet(key, "vt", visibilityTimeout)
	tx.HSet(key, "delay", delay)
	tx.HSet(key, "maxsize", maxSize)
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	return r.GetQueueAttributes(queueName)
}

// SendMessageWithDelay adds a message to a queue that will not be visible until
// after the specified delay.
func (r *RedisSMQ) SendMessageWithDelay(queueName string, message string, delay uint64) (string, error) {
	return r.sendMessage(queueName, message, delay, false)
}

// SendMessage adds a message to a queue using the default delay value for the queue
func (r *RedisSMQ) SendMessage(queueName string, message string) (string, error) {
	return r.sendMessage(queueName, message, 0, true)
}

func (r *RedisSMQ) sendMessage(queueName string, message string, delay uint64, useQueueDelay bool) (string, error) {
	if err := validateQueueName(queueName); err != nil {
		return "", err
	}

	if !useQueueDelay {
		if err := validateDelay(delay); err != nil {
			return "", err
		}
	}

	qd, err := r.getQueue(queueName, true)
	if err != nil {
		return "", err
	}

	if qd.MaxSize != -1 && int64(len(message)) > qd.MaxSize {
		return "", ErrMessageTooLong
	}

	key := r.namespace + queueName

	dly := delay
	if useQueueDelay {
		dly = qd.Delay
	}

	tx := r.client.TxPipeline()
	tx.ZAdd(key, redis.Z{
		Score:  float64(qd.TimestampMS + dly*1000),
		Member: qd.UniqueID,
	})
	tx.HSet(key+":Q", qd.UniqueID, message)
	tx.HIncrBy(key+":Q", "totalsent", 1)
	if _, err := tx.Exec(); err != nil {
		return "", err
	}

	return qd.UniqueID, nil
}

// PopMessage removes and returns the next message from the given queue. Use
// caution as this immediately deletes the returned message from the queue.
func (r *RedisSMQ) PopMessage(queueName string) (*QueueMessage, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	qd, err := r.getQueue(queueName, false)
	if err != nil {
		return nil, err
	}

	key := r.namespace + queueName
	timeMS := strconv.FormatUint(qd.TimestampMS, 10)

	cmd := r.popMessageScript.Run(r.client, []string{key, timeMS})

	return r.handleReceivedMessage(cmd)
}

// ReceiveMessageWithTimeout returns the next message from the given queue. The message
// will remain in the queue, but cannot be received again until after the specified
// visibilityTimeout has elapsed.
func (r *RedisSMQ) ReceiveMessageWithTimeout(queueName string, visibilityTimeout uint64) (*QueueMessage, error) {
	return r.receiveMessage(queueName, visibilityTimeout, false)
}

// ReceiveMessage returns the next message form the given queue. The queue's default
// visibility timeout will be used.
func (r *RedisSMQ) ReceiveMessage(queueName string) (*QueueMessage, error) {
	return r.receiveMessage(queueName, 0, true)
}

func (r *RedisSMQ) receiveMessage(queueName string, visibilityTimeout uint64, useQueueTimeout bool) (*QueueMessage, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	if !useQueueTimeout {
		if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
			return nil, err
		}
	}

	qd, err := r.getQueue(queueName, false)
	if err != nil {
		return nil, err
	}

	vt := visibilityTimeout
	if useQueueTimeout {
		vt = qd.VisibiltyTimeout
	}

	key := r.namespace + queueName
	visibleTime := strconv.FormatUint(qd.TimestampMS+vt*1000, 10)
	timeMS := strconv.FormatUint(qd.TimestampMS, 10)

	cmd := r.receiveMessageScript.Run(r.client, []string{key, timeMS, visibleTime})

	return r.handleReceivedMessage(cmd)
}

func (r *RedisSMQ) handleReceivedMessage(cmd *redis.Cmd) (*QueueMessage, error) {
	cmdVals, ok := cmd.Val().([]interface{})
	if !ok {
		return nil, errors.New("invalid received message response")
	}

	if len(cmdVals) == 0 {
		return nil, nil
	}

	messageID, err := toString(cmdVals[0])
	if err != nil {
		return nil, errors.New("invalid message ID retrieved")
	}
	message, err := toString(cmdVals[1])
	if err != nil {
		return nil, errors.New("invalid message retrieved")
	}
	receivedCount, err := toUint64(cmdVals[2])
	if err != nil {
		return nil, errors.New("invalid received count retrieved")
	}
	firstReceivedAt, err := toInt64(cmdVals[3])
	if err != nil {
		return nil, errors.New("invalid first received at retrieved")
	}

	sentTimeUS, err := strconv.ParseInt(messageID[0:10], 36, 64)
	if err != nil {
		return nil, errors.New("could not parse sent time from message id")
	}

	return &QueueMessage{
		ID:              messageID,
		Message:         message,
		SentAt:          time.Unix(0, sentTimeUS*1000),
		FirstReceivedAt: time.Unix(0, firstReceivedAt*1000000),
		ReceivedCount:   receivedCount,
	}, nil
}

// ChangeMessageVisibility changes the visibility timer of a single message. The
// time when the message will be visible again is calculated from the current time
// plus the specified visibility timeout in seconds.
func (r *RedisSMQ) ChangeMessageVisibility(queueName string, messageID string, visibilityTimeout uint64) error {
	qd, err := r.getQueue(queueName, false)
	if err != nil {
		return err
	}

	queueKey := r.namespace + queueName
	time := strconv.FormatUint(qd.TimestampMS+visibilityTimeout*1000, 10)

	exists, err := r.changeMessageVisibilityScript.Run(r.client, []string{queueKey, messageID, time}).Bool()
	if err != nil {
		return err
	}

	if !exists {
		return ErrMessageNotFound
	}

	return nil
}

// DeleteMessage removes the message with the given ID from a queue
func (r *RedisSMQ) DeleteMessage(queueName string, messageID string) error {
	if err := validateQueueName(queueName); err != nil {
		return err
	}

	if err := validateMessageID(messageID); err != nil {
		return err
	}

	key := r.namespace + queueName

	tx := r.client.TxPipeline()
	zremCmd := tx.ZRem(key, messageID)
	hdelCmd := tx.HDel(key+":Q", messageID, messageID+":rc", messageID+":fr")
	if _, err := tx.Exec(); err != nil {
		return err
	}

	if zremCmd.Val() != 1 || hdelCmd.Val() == 0 {
		return ErrMessageNotFound
	}

	return nil
}

// generateRandomBytes returns securely generated random bytes. It will return an
// error if the system's secure random number generator fails to function
// correctly, in which case the caller should not continue.
func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

// makeID returns a securely generated random string. It will return an error if
// the system's secure random number generator fails to function correctly, in
// which case the caller should not continue.
func makeID(n int) (string, error) {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	bytes, err := generateRandomBytes(n)
	if err != nil {
		return "", err
	}
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes), nil
}

func validateQueueName(str string) error {
	re := regexp.MustCompile(`^([a-zA-Z0-9_-]){1,160}$`)
	if !re.MatchString(str) {
		return ErrInvalidQueueName
	}

	return nil
}

func validateMessageID(str string) error {
	re := regexp.MustCompile(`^([a-zA-Z0-9:]){32}$`)
	if !re.MatchString(str) {
		return ErrInvalidMessageID
	}

	return nil
}

func validateVisibilityTimeout(val uint64) error {
	if val > 9999999 {
		return ErrInvalidVisibilityTimeout
	}

	return nil
}

func validateDelay(val uint64) error {
	if val > 9999999 {
		return ErrInvalidDelay
	}

	return nil
}

func validateMaxSize(val int64) error {
	if (val < 1024 || 65535 < val) && val != -1 {
		return ErrInvalidMaxSize
	}

	return nil
}

func toString(value interface{}) (string, error) {
	switch val := value.(type) {
	case string:
		return val, nil
	default:
		err := fmt.Errorf("value with type %T cannot be converted to string", val)
		return "", err
	}
}

func toUint64(value interface{}) (uint64, error) {
	switch val := value.(type) {
	case int64:
		return uint64(val), nil
	case string:
		return strconv.ParseUint(val, 10, 64)
	default:
		err := fmt.Errorf("value with type %T cannot be converted to uint64", val)
		return 0, err
	}
}

func toInt64(value interface{}) (int64, error) {
	switch val := value.(type) {
	case int64:
		return val, nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		err := fmt.Errorf("value with type %T cannot be converted to int64", val)
		return 0, err
	}
}
