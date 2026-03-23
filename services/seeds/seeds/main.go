package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	seed "github.com/piwonka/synphony/proto/seeds"
	"github.com/piwonka/synphony/scripts/lib/helpers"
	kafkautils "github.com/piwonka/synphony/scripts/lib/kafka"
	"google.golang.org/protobuf/proto"
)

var TOPIC_LAG_THRESHOLD = 10 // produce seeds only when below this threshold to avoid producing a large quantity of seeds when the pipeline stalls and cant consume them all

/* Runs on startup, starts up a goroutine that listens for software interrupts and closes kafka readers if they happen*/
func init() {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		kafkautils.CloseAllReaders()
		os.Exit(0)
	}()
}

/* Reads a protobuf-encoded Seed Message from disk using the specified path. Returns (nil, nil) if the file does not exist.*/
func LoadLastSeedProto(path string) (*seed.Seed, error) {
	b, err := os.ReadFile(path) // read the file
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read state file %q: %w", path, err)
	}
	if len(b) == 0 { // if file is empty
		// Treat empty as "no state"
		return nil, nil
	}

	var msg seed.Seed
	if err := proto.Unmarshal(b, &msg); err != nil { // parse and unmarshal the file
		return nil, fmt.Errorf("unmarshal protobuf state %q: %w", path, err)
	}
	return &msg, nil // return seed state
}

/* Writes seed state as protobuf to disk "atomically". It writes to <path>.tmp and renames to <path>, replacing old state files (atomic on same filesystem because of os write behavior). returns an error if unsuccessful. Technically this is not proper atomicity but it works for this simple usecase */
func StoreLastSeedProtoAtomic(path string, msg *seed.Seed) error {
	if msg == nil {
		return fmt.Errorf("msg must not be nil")
	}

	b, err := proto.Marshal(msg) // marshal seed message to byte[]
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil { // create directory if it doesnt exist
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil { // write file to a tmp file  ( replacing old tmp files)
		return fmt.Errorf("write tmp %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil { // rename the file to the proper seed file, replacing old state files
		return fmt.Errorf("rename %q -> %q: %w", tmp, path, err)
	}
	return nil
}

/*
Reads from all worker kafka topics until EOF and sets the last message as the last worker seed, restoring the state any previous seed services had If nothing is on kafka, it will return an empty map, ensuring correct initialization for the first run.
*/
func restore_seed_state() (*seed.Seed, error) {
	last_seed, err := LoadLastSeedProto("/seed/state.tmp")
	if err != nil {
		log.Println("Could not restore seed state: %w", err)
	}
	return last_seed, err
}

/*
The service responsible for generating work items (seed messages) for the entire pipeline.
These random values are running numbers that can be used as seed or directly as random values and are specific to this worker and pipeline id.
They are derived from the master seed and will not differ between runs, as long as the parameters are the same.
*/
func main() {
	// retrieve necessary information from envs
	masterseed := os.Getenv("MASTER_SEED")
	numberOfSeeds, err := strconv.Atoi(os.Getenv("NUMBER_OF_SEEDS"))
	trackSeedCount := true
	if err != nil { // if number of seeds is set to a negative value, that signifies infinite seed generation, set trackSeedCount to false
		numberOfSeeds = -1
		trackSeedCount = false
	}
	kafka_brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")

	// retrieve previous state ( enables replayability of this service on failure
	log.Println("Trying to restore previous state from Kafka...")
	last_seed_proto, err := restore_seed_state() // read last known seed from peristent storage
	last_worker_seed := ""
	if err == nil && last_seed_proto != nil { // set last worker seed to last known state
		log.Printf(
			"Last Known Seed is %s created at %d from master seed %s.",
			last_seed_proto.Seed,
			int(last_seed_proto.CreationTimestamp),
			last_seed_proto.MasterSeed,
		)
		last_worker_seed = last_seed_proto.Seed
	}
	log.Println("If there was state it has been restored!")
	log.Println("Seed service is up!")
	topic := os.Getenv("SEED_TOPIC")
	for { // create one seed per iteration
		// compute current topic lag between all workers, if it cant be computed or times out, use 0 and just keep producing
		lag, err := kafkautils.GetGlobalLag(
			kafka_brokers,
			topic,
			"seed-reader-group",
		)
		if err != nil {
			log.Printf(
				"Can not get Topic Depth:%v\nDefaulting to 0, produce more Seeds",
				err.Error(),
			)
			lag = 0
		}
		log.Printf("Topic Depth for %s is %d", topic, lag)

		if lag < int64(
			TOPIC_LAG_THRESHOLD,
		) { // as long as the lag is under the threshold, produce more seeds
			log.Println("Kafka Lag is under the Threshold. Writing more Seeds to topic.")
			workerSeed, err := helpers.GenerateWorkerSeed(
				last_worker_seed,
				masterseed,
			) // generate a worker seed using either the previously recovered state, or the master seed if no state was present
			if err != nil {
				log.Printf("could not generate workerseed: %v", err)
				continue
			}
			seed_message := seed.Seed{ // generate a protobuf message from the generated seed
				MasterSeed:        masterseed,
				Seed:              workerSeed,
				CreationTimestamp: time.Now().UnixMilli(),
			}
			err = StoreLastSeedProtoAtomic(
				"/seed/state.tmp",
				&seed_message,
			) // store the seed within state
			if err != nil {
				log.Printf(
					"error storing seed state, repeating seed generation, no seed is send to kafka for this loop iteration: %v",
					err,
				)
				continue // seed state can not be stored, continuing with writing the seed would skip seeds in the seed sequence, repeat the seed generation and try storing it again.
			}
			log.Println("Writing to Kafka")
			kafkautils.WriteToKafkaWithRetry( // write the seed message to kafka
				kafka_brokers,
				topic,
				&seed_message,
			)
			last_worker_seed = workerSeed
			log.Println(
				"Seed written successfully.",
			)

			// ensure we do not produce more seeds than configured
			if trackSeedCount {
				numberOfSeeds--
				if numberOfSeeds == 0 {
					log.Printf(
						"All %s seeds have been created. Application is finished.",
						os.Getenv("NUMBER_OF_SEEDS"),
					)
					os.Exit(0) // exit the loop
				}
			}

		}

	}
}
