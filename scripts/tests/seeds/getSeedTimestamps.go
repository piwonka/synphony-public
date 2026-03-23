package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	seed "github.com/piwonka/synphony/proto/seeds"
	kafkautils "github.com/piwonka/synphony/scripts/lib/kafka"
)

func main() {
	groupid := "test-run-" + uuid.NewString()
	seed_topic := os.Args[1]
	kafka_brokers := strings.Split(os.Args[2], ",")
	numberOfSeeds, err := strconv.Atoi(os.Args[3])
	log.Printf(
		"ENVS:%s,%s,%s",
		groupid,
		seed_topic,
		kafka_brokers,
	)
	if err != nil {
		log.Printf(
			"Could not parse Number of Seeds:%v",
			err,
		)
		os.Exit(1)
	}
	var firstSeed *seed.Seed
	var lastSeed *seed.Seed
	for i := range numberOfSeeds {
		seedMessage := kafkautils.ReadFromKafkaWithRetry[*seed.Seed](
			kafka_brokers,
			seed_topic,
			groupid,
		)
		if i == 0 || firstSeed.CreationTimestamp > seedMessage.CreationTimestamp {
			firstSeed = seedMessage
		}
		if i == 0 || lastSeed.CreationTimestamp < seedMessage.CreationTimestamp {
			lastSeed = seedMessage
		}
	}
	log.Printf(
		"First Seed:%s\nTimestamp:%d\n-----------\nLast Seed:%s\nTimestamp:%d",
		firstSeed.Seed,
		firstSeed.CreationTimestamp,
		lastSeed.Seed,
		lastSeed.CreationTimestamp,
	)
	os.Exit(0)
}
