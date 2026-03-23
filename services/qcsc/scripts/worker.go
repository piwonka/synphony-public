package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	metadata "github.com/piwonka/synphony/proto/metadata"
	"github.com/piwonka/synphony/scripts/lib/helpers"
	kafkautils "github.com/piwonka/synphony/scripts/lib/kafka"
	minioutils "github.com/piwonka/synphony/scripts/lib/minio"
)

/*Struct type that represents Blender work outputs ( QC DECISIONS)*/
type BlenderEvent struct {
	kind     string
	seed     string
	filePath string
	decision *metadata.QCDecision
}

/*Runs at startup, starts a goroutine that listens for software interrupts and if they happen closes all kafka readers gracefully*/
func init() {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		kafkautils.CloseAllReaders()
		os.Exit(0)
	}()
}

/* write the created artifact to minio and update and write metadata to kafka, commit work item at completion*/
func writeData(
	config helpers.Config,
	file_path string,
	seed string,
	metadata *metadata.Metadata) {

	// decide the bucket and topic for the data based on wether the scene passed all qc checks
	correct_bucket := ""
	correct_topic := ""
	if metadata.PassedQc {
		correct_bucket = os.Getenv("SCENE_QC_PASS_BUCKET")
		correct_topic = os.Getenv("SCENE_QC_PASS_TOPIC")
	} else {
		correct_bucket = os.Getenv("SCENE_QC_FAIL_BUCKET")
		correct_topic = os.Getenv("SCENE_QC_FAIL_TOPIC")
	}
	log.Printf(
		"Job with seed %s will be put into topic %s and bucket %s according to the qc decision: %t",
		seed,
		correct_topic,
		correct_bucket,
		metadata.PassedQc,
	)

	// Try writing to minio
	minio_path := minioutils.WriteToMinioWithRetry(
		config.MinioEndpoint,
		config.Minio_Access_Key,
		config.Minio_Secret_Key,
		file_path,
		correct_bucket,
	)
	log.Printf("File %s was written to MinIO", file_path)

	// update metadata object
	metadata.Filehandle = minio_path
	// write metadata object to kafka
	kafkautils.WriteToKafkaWithRetry(config.Kafka_Brokers, correct_topic, metadata)
	log.Printf("Metadata was written to Kafka")
	// Commit message to kafka, signifying the work items completion
	n, err := kafkautils.CommitMessages(
		config.Kafka_Brokers,
		os.Getenv("SCENES_TOPIC"),
		"scenes-reader-group",
	)
	if err != nil {
		log.Printf(
			"Could not commit Offset. Offsets will be saved on the next successful commit. Otherwise this message will be reprocessed at a later time.",
		)
	}
	log.Printf("Committed %d Offsets.", n)

}

/* Creates a blender job and passes it to the bootstrap component through stdin */
func sendJobToBlender(
	inputStream *io.WriteCloser,
	inputFile string,
	seed string,
) {
	job := fmt.Sprintf("JOB %s %s\n", inputFile, seed)
	log.Printf("Forwarding this Job to the blender bootstrap: %s", job)
	_, err := io.WriteString(*inputStream, job)
	if err != nil {
		log.Printf("could not write job to stdin, skipping this scene: %s", job)
	}
}

/* Parse BlenderEvents from Blender output lines*/
func handleQualityGate(line string,
) BlenderEvent {
	results := strings.Split(line, ",")            // split results
	decision, err := strconv.ParseBool(results[3]) // parse the decision outcome
	if err != nil {
		log.Printf("qcdecision could not be parsed. Defaulting to decision: false - %v", err)
	}
	// create qcDecision object
	qcDecision := &metadata.QCDecision{
		Type:              results[2],
		Decision:          decision,
		CreationTimestamp: time.Now().UnixMilli(),
		Data:              results[4],
	}
	// create BlenderEvent from qcDecision outputs
	ev := BlenderEvent{
		kind:     "RESULT",
		seed:     results[1],
		decision: qcDecision,
	}
	log.Printf(
		"Quality Gate %s for seed %s decision: %t",
		qcDecision.Type,
		ev.seed,
		qcDecision.Decision,
	)
	return ev
}

/*Handle "DONE" events that signify all user scripts are finished for this scene, return a blender event containing job information*/
func handleBlenderResponse(
	line string,
) BlenderEvent {
	log.Printf("Done Flag Received: %s", line)
	results := strings.Split(line, ",")
	return BlenderEvent{
		kind:     "DONE",
		seed:     results[1],
		filePath: results[2],
	}
}

/*Read metadata from minio, use blender to compute quality gates for the artifacts linked to the metadata*/
func streamData(
	conf helpers.Config,
	stdin *io.WriteCloser,
	blenderChan chan BlenderEvent,
) {
	for { // repeat until death
		// retrieve metadata from kafka
		metadata := kafkautils.ReadFromKafkaWithRetry[*metadata.Metadata](
			conf.Kafka_Brokers,
			os.Getenv("SCENES_TOPIC"),
			"scenes-reader-group",
		)
		log.Printf("Scene was read: %s", metadata.Filehandle)

		// Copy the .blend file from minio to local disc
		filename := filepath.Base(
			metadata.Filehandle,
		) // example for metadata.Filehandle: s3://bucket/file
		scene_path := "/tmp/minio/" + filename
		minioutils.ReadFromMinioWithRetry(
			conf.MinioEndpoint,
			conf.Minio_Access_Key,
			conf.Minio_Secret_Key,
			filename,
			os.Getenv("SCENES_MINIO_BUCKET"),
			scene_path,
		)
		// create job and send it to the bootstrap component
		sendJobToBlender(stdin, scene_path, metadata.Seed)
		var res BlenderEvent
		for { // parse blender responses for each user script
			res = <-blenderChan            // wait for user script result
			if res.seed != metadata.Seed { // defensive programming, this does not happen
				log.Printf(
					"Unexpected Result: seed: %s expected: %s. Ignoring...",
					res.seed,
					metadata.Seed,
				)
				continue
			}

			if res.kind == "DONE" { // if the blender response is DONE, all user scripts have finished and all blender computations for this work item have ended
				break // stop the loop
			}

			if res.kind == "RESULT" &&
				res.decision != nil { // if the blender response is RESULT it signifies a singular QC decision -> add it to the metadata
				metadata.Qc = append(metadata.Qc, res.decision)
			}
		}

		// compute the information wether the scene has passed all qc checks
		metadata.PassedQc = true
		for _, d := range metadata.Qc {
			metadata.PassedQc = metadata.PassedQc && d.Decision
		}
		// write the data and continue with the next work item
		writeData(conf, res.filePath, res.seed, metadata)
	}
}

// The main function that registers the directory watcher and launches the blender command
func main() {
	conf := helpers.ReadEnvs()
	// Spin up blender running the bootstrap script waiting for jobs from this worker
	cmd := exec.Command(
		"blender",
		"--background",
		"--python",
		"/workspace/bootstrap.py",
		"--",
		"--f",
		"/workspace/", // directory that contains all user scripts for qc
	)
	// set up stdout streaming for logging and results
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	merged := io.MultiReader(stdout, stderr)
	// set up stdin for passing jobs to blender
	stdin, _ := cmd.StdinPipe()

	// Create a channel that is used to signify waiting for a result
	blenderChan := make(chan BlenderEvent, 64)
	// Set up logging passthrough and result handler as a goroutine that permanently scans blender output for results
	go func() {
		scanner := bufio.NewScanner(merged)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "DONE") { // if the work item is completed
				blenderChan <- handleBlenderResponse(line) // handle the done message and then write the data
			} else if strings.HasPrefix(line, "RESULT") {
				blenderChan <- handleQualityGate(line) // add the result to the qc decisions within the metadata
			}
			fmt.Println("[BLENDER]", line) // prints Blender logs
		}
	}()

	defer stdout.Close()
	defer stderr.Close()

	// start the blender worker check if startup successful
	if err := cmd.Start(); err != nil {
		log.Fatalf("Blender exited with error:%v", err)
	}
	// start reading work items until this process ends
	streamData(conf, &stdin, blenderChan)

	if err := cmd.Wait(); err != nil {
		log.Fatalf("Blender exited with error:%v", err)
	}
}
