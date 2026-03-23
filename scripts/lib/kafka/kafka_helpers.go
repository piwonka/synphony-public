//go:build !franz_disabled
// +build !franz_disabled

package kafkautils

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

var mu sync.Mutex // guard against concurrent access to the caches

// define some globals used for potential backoff logic TODO: used? idk
var minDelay = 200 * time.Millisecond
var maxDelay = 10 * time.Second
var jitter = 100 * time.Millisecond

var BATCH_SIZE = 50

// reuse readers and writers where possible
var readerCache = make(map[string]*kafka.Reader)
var writerCache = make(map[string]*kafka.Writer)

// clients for external libraries used for lag check
var kgoClient *kgo.Client
var kadmClient *kadm.Client

var pendingCommitMessages = make(
	[]kafka.Message,
	0,
	BATCH_SIZE,
) // Store uncommitted messages manually

/*Retrieve admin clients for external libraries, if they are not yet initialized, create them and return them*/
func getOrCreateAdminClients(brokers []string) (*kgo.Client, *kadm.Client, error) {
	mu.Lock()
	defer mu.Unlock()
	if kgoClient == nil {
		client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		kadmClient = kadm.NewClient(client)
		kgoClient = client
		return client, kadmClient, err
	}
	return kgoClient, kadmClient, nil
}

/*Retrieve the Kafka Reader for a specific topic and group. If no reader exists for the group-topic pair , create it and return it, after storing it in the readerCache*/
func getOrCreateReader(brokers []string, topic string, groupID string) *kafka.Reader {
	key := topic + "-" + groupID
	mu.Lock()
	defer mu.Unlock()
	if r, ok := readerCache[key]; ok { // check if the key for this groupid and topic exists in the cache
		return r
	}
	r := kafka.NewReader( // create new reader
		kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			MaxWait: 501 * time.Millisecond, // ensure this does not exactly stop waiting when the next data point arrives
			// StartOffset:    kafka.FirstOffset,      // flag is ignored once an offset has been committed for the group
			CommitInterval: 0,
		},
	)
	readerCache[key] = r // store new reader in cache
	return r
}

/*Retrieve the Kafka Writer for a specific topic. If no writer exists for this topic, create it and return it after storing it in the writerCache*/
func getOrCreateWriter(brokers []string, topic string) *kafka.Writer {
	mu.Lock()
	defer mu.Unlock()
	if w, ok := writerCache[topic]; ok { // check if a writer for this topic exists
		return w
	}
	w := kafka.NewWriter(kafka.WriterConfig{ // create new writer
		Brokers:      brokers,
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	})
	writerCache[topic] = w // store it in the cache
	return w
}

/*
Invokes the passed function until it Succeeds to generate a Result, with varying backoff timer. Returns the Result or a Nullvalue and the corresponding error.
*/
func retryWithResult[T proto.Message](fn func() (T, error)) T {
	var result T
	var err error
	sleep := minDelay
	for {
		result, err = fn()
		if err == nil { // invoke function, if result is present, return the result
			return result
		} else { // otherwise back off
			log.Printf("Kafka Access Failed. Backing Off before Retrying due to Error: %v", err)

			j := time.Duration(rand.Int63n(int64(jitter)*2+1)) - jitter
			sleep += j
			if sleep <= minDelay || sleep >= maxDelay {
				sleep = minDelay
			}
			time.Sleep(sleep)

		}
	}
}

/* Writes a message of type T to the specified kafka topic with varying backoff duration. */
func WriteToKafkaWithRetry[T proto.Message](
	brokers []string,
	topic string,
	message T,
) {
	retryWithResult(
		func() (T, error) { // lambda, throw away return values that exist because of the generic function signature
			var zero T
			return zero, writeToKafka(brokers, topic, message)
		},
	)
}

/* Writes a Message of Type T to the specified kafka broker and topic synchronously, blocks until successful write. Returns an error or nil.*/
func writeToKafka[T proto.Message](
	brokers []string,
	topic string,
	message T,
) error {
	// set up kafka writer
	writer := getOrCreateWriter(brokers, topic)

	// Prepare and Send message
	marshalled_message, err := proto.Marshal(message) //protobuf blob
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key: []byte(
				uuid.New().String(),
			), // each kafka message needs a key, use a unique identifier, for kafka internals
			Value: marshalled_message,
		},
	)
	return err
}

/* Reads a message of type T from the specified kafka topic using the specified group with varying backoff duration.
*  Returns the protobuf unmarshalled Protobuf Message.
 */
func ReadFromKafkaWithRetry[T proto.Message](
	brokers []string,
	topic string,
	groupID string,
) T {
	return retryWithResult(
		func() (T, error) { // lambda, return result ( unmarshalled protobuf blob)
			return readFromKafka[T](brokers, topic, groupID)
		},
	)
}

/* Commits the Stack of currently pending Messages, marking the completion of the work done on the Messages. Returns the number of committed Messages.
*  This function does not have to be retried, it does not fit the retry with result schema with proto.Message
*   and this call failing just means we process some datapoints twice, which overwrite each other.
 */
func CommitMessages(brokers []string, topic string, groupID string) (int, error) {
	r := getOrCreateReader(brokers, topic, groupID)
	mu.Lock()
	batchSize := len(pendingCommitMessages)
	if batchSize == 0 {
		mu.Unlock()
		return 0, nil
	}
	mu.Unlock()
	err := r.CommitMessages(context.Background(), pendingCommitMessages...)
	if err != nil {
		return 0, err
	}
	mu.Lock()
	pendingCommitMessages = pendingCommitMessages[:0]
	mu.Unlock()
	return batchSize, nil
}

/* Reads a message of type T from a specific kafka topic using the specified group. Returns the unmarshalled Message and an error, if present.*/
func readFromKafka[T proto.Message](brokers []string, topic string, groupID string) (T, error) {
	var zero T // declare message variable of type T
	ctx := context.Background()
	reader := getOrCreateReader(brokers, topic, groupID)
	message_bytes, err := reader.FetchMessage(ctx) // retrieve the message byte[] from kafka
	if err != nil {                                // if the message could not be read, return an error
		return zero, err
	}
	mu.Lock()
	pendingCommitMessages = append(
		pendingCommitMessages,
		message_bytes,
	) // append the message to the pending messages
	mu.Unlock()
	// Instantiate an empty object of the message type (which would normally not be possible) using reflection
	message := reflect.New(reflect.TypeOf(zero).Elem()).Interface().(T)
	err = proto.Unmarshal(
		message_bytes.Value,
		message,
	) // unmarshal the protobuf blob to the message object, creating a proper message of type T
	if err != nil { // if unmarshalling failed, return the zero element and an error
		return zero, err
	}
	return message, err // return the message and nil
}

/* Get the current global kafka lag on a topic for the specified group. */
func GetGlobalLag(brokers []string, topic string, groupID string) (int64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()                                   // cancel this functions execution if it takes more than a second to complete
	_, kadm, err := getOrCreateAdminClients(brokers) // retrieve admin clients
	if err != nil {
		return 0, fmt.Errorf("failed to create franz and kadm clients: %w", err)
	}

	endOffsets, err := kadm.ListEndOffsets(ctx, topic) // get the end offsets of a topic.
	if err != nil {
		return 0, fmt.Errorf("failed to list end offsets: %w", err)
	}
	startOffsets, err := kadm.ListStartOffsets(ctx, topic) // get the start offsets of a topic
	if err != nil {
		return 0, fmt.Errorf("failed to list start offsets:%w", err)
	}

	groupsResp, err := kadm.FetchOffsetsForTopics(
		ctx,
		groupID,
		topic,
	) // retrieve consumer offsets for a group on a specific topic
	if err != nil {
		return 0, fmt.Errorf(
			"failed to fetch group offsets for group %s and topic %s: %w",
			groupID,
			topic,
			err,
		)
	}

	// Compute the lag
	totalLag := int64(0)
	for _, eo := range endOffsets.Offsets().Sorted() { // iterate over the partition end offsets of the topic
		if eo.Topic != topic { // double check that we use the correct topic	 TODO: remove this, does not do anything
			log.Println("Wrong topic selected during lag check, continuing")
			continue
		}

		so, _ := startOffsets.Lookup(
			topic,
			eo.Partition,
		) // get the start offsets for this topic partition
		start := so.Offset
		commit := start
		committed, ok := groupsResp.Lookup(
			topic,
			eo.Partition,
		) // check the current committed offset for each consumer group that consumes this topic partition
		if ok &&
			committed.At >= 0 { //if an offset was committed, note the number of committed messages
			commit = committed.At
			// If kafka retention settings moved start forward beyond committed, clamp start to commit.
			if commit < start {
				commit = start
			}
		}

		// compute partition lag
		lag := eo.At - commit
		if lag < 0 {
			lag = 0
		}
		// add partition lag to total lag
		totalLag += lag
	}

	return totalLag, nil
}

/*Cleanup function, close all readers still in the caches and end the connection within kafka admin clients.*/
func CloseAllReaders() {
	mu.Lock()
	defer mu.Unlock()
	for key, r := range readerCache {
		if err := r.Close(); err != nil {
			log.Printf("Error closing reader %s: %v", key, err)
		}
		delete(readerCache, key)
	}
	for key, w := range writerCache {
		if err := w.Close(); err != nil {
			log.Printf("Error closing reader %s: %v", key, err)
		}
		delete(readerCache, key)
	}
	if kgoClient != nil {
		kgoClient.Close()
	}
	if kadmClient != nil {
		kadmClient.Close()
	}
}
