package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	metadata "github.com/piwonka/synphony/proto/metadata"
	seed "github.com/piwonka/synphony/proto/seeds"
	"github.com/piwonka/synphony/scripts/lib/helpers"
	kafkautils "github.com/piwonka/synphony/scripts/lib/kafka"
	minioutils "github.com/piwonka/synphony/scripts/lib/minio"
)

/*Contains information about blender outputs*/
type BlenderEvent struct {
	kind       string
	outputFile string
	seed       string
}

/*
* Set up a Hook to Close all Readers on Program shutdown (software interrupts)
 */
func init() {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		kafkautils.CloseAllReaders()
		os.Exit(0)
	}()
}

/*
* Write the created File to MinIO, update its metadata and write it to Kafka, then commit the message to signify work item completion
 */
func writeData(conf helpers.Config, outputFile string, metadata *metadata.Metadata) {
	// Blocking Write to Minio
	minio_path := minioutils.WriteToMinioWithRetry(
		conf.MinioEndpoint,
		conf.Minio_Access_Key,
		conf.Minio_Secret_Key,
		outputFile,
		os.Getenv("SCENES_TOPIC"),
	)
	log.Printf("File %s written to minio successfully", outputFile)

	// update metadata object
	metadata.CreationTimestamp = time.Now().Unix()
	metadata.Filetype = ".blend"
	metadata.Filehandle = minio_path

	// write metadata object to kafka
	kafkautils.WriteToKafkaWithRetry(
		conf.Kafka_Brokers,
		os.Getenv("SCENES_TOPIC"),
		metadata,
	)
	log.Printf("Metadata written to topic %s", os.Getenv("SCENES_TOPIC"))
	// commit work item to signify completion
	n, err := kafkautils.CommitMessages(
		conf.Kafka_Brokers,
		os.Getenv("SEED_TOPIC"),
		"seed-reader-group",
	)
	if err != nil {
		log.Printf(
			"Could not commit Offset. Offsets will be saved on the next successful commit. Otherwise this message will be reprocessed at a later time.",
		)
	}
	log.Printf("Committed %d Offsets.", n)
}

/*Parse the response from a blender job into a Blender Event*/
func parseBlenderResponse(line string) BlenderEvent {
	split := strings.Split(line, " ")
	return BlenderEvent{
		kind:       split[0],
		outputFile: split[1],
		seed:       split[2],
	}
}

/*Create a job using the arguments and pass it to the bootstrap component using stdin*/
func sendJobToBlender(
	inputStream *io.WriteCloser,
	outputFile string,
	workerSeed string,
	seed string,
) {
	_, err := io.WriteString(
		*inputStream,
		fmt.Sprintf("JOB %s %s %s \n", outputFile, workerSeed, seed),
	)
	if err != nil {
		log.Printf("Error sending job to blender: %v", err)
	}
}

/* Read work items from kafka, perform scene generation by sending jobs to blender, write the data to kafka and minio*/
func streamData(
	conf helpers.Config,
	stdin *io.WriteCloser,
	envCh chan BlenderEvent) {
	seed_topic := os.Getenv("SEED_TOPIC")
	log.Printf("Seed Topic %s, Kafka brokers %s", seed_topic, os.Getenv("KAFKA_BROKERS"))
	for { // repeat until death
		// Read the seed for this worker
		seedMessage := kafkautils.ReadFromKafkaWithRetry[*seed.Seed](
			strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
			os.Getenv("SEED_TOPIC"),
			"seed-reader-group",
		)
		log.Printf("Seed %s was received and will be passed to the blender worker",
			seedMessage.Seed,
		)
		// generate stage seed for this work item using XOR with the stage seed provided through envs
		jobSeed, err := helpers.GenerateWorkerSeedForStage(
			seedMessage.Seed,
			os.Getenv("SCENE_STAGE_SEED"),
		)
		if err != nil {
			log.Printf("could not generate a seed for this job:%v", err)
			continue
		}

		// compute the output file name
		output_path := "/output/" + "terrain_" + jobSeed + ".blend"

		// create the output directory if not present
		err = os.MkdirAll("/output", 0755)
		if err != nil {
			log.Printf("Could not create output directory: %v", err)
		}
		// create the metadata for this work item
		metadata := metadata.Metadata{
			Seed:              seedMessage.Seed,
			SceneSeed:         jobSeed,
			CreationTimestamp: time.Now().Unix(),
			Filetype:          ".blend",
		}
		// send blender job that creates artifact for this work item
		sendJobToBlender(stdin, output_path, jobSeed, metadata.Seed)
		ev := <-envCh                 // Block until Blender Responds with Result for the Job
		if ev.seed != metadata.Seed { // defensive programming, this does not happen
			log.Printf(
				"Unexpected Result: seed: %s expected: %s. Ignoring...",
				ev.seed,
				metadata.Seed,
			)
			continue
		}
		// write the data to kafka and minio, signify work item completion, continue with next work item
		writeData(conf, ev.outputFile, &metadata)

	}
}

/* The main function that invokes the bootstrap script running in blender and starts the Streaming of data*/
func main() {
	// read envs
	conf := helpers.ReadEnvs()
	// spawn blender process running the bootstrap script
	cmd := exec.Command(
		"blender",
		"--background",
		"--python",
		"/workspace/bootstrap.py",
		"--",
		"--f",
		"generate_scenes.py", // user script path
	)
	// set up stdin and stdout passthrough for blender
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	defer stdout.Close()
	defer stderr.Close()
	merged := io.MultiReader(stdout, stderr)

	stdin, _ := cmd.StdinPipe()

	// create a channel that is used to signify waiting for a work item
	envCh := make(chan BlenderEvent, 8)

	// Set up logging passthrough and result handler as a goroutine running in the background
	go func() {
		scanner := bufio.NewScanner(merged) // continually read blender outputs
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(
				line,
				"RESULT",
			) { // if the output has a RESULT flag, pass it to the channel to signify blender work for the current work item is done
				envCh <- parseBlenderResponse(line)
			}
			fmt.Println("[BLENDER]", line) // prints Blender logs
		}
	}()

	// start the blender worker check if startup successful
	if err := cmd.Start(); err != nil {
		log.Fatalf("Blender exited with error:%v", err)
	}
	// start streaming data from kafka
	streamData(conf, &stdin, envCh)

	if err := cmd.Wait(); err != nil {
		log.Fatalf("Blender exited with error:%v", err)
	}
}
