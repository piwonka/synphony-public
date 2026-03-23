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
	"strings"
	"time"

	metadata "github.com/piwonka/synphony/proto/metadata"
	"github.com/piwonka/synphony/scripts/lib/helpers"
	kafkautils "github.com/piwonka/synphony/scripts/lib/kafka"
	minioutils "github.com/piwonka/synphony/scripts/lib/minio"
)

/* Blender Job response type */
type BlenderEvent struct {
	kind       string
	outputFile string
	seed       string
}

/* Runs on process start, creates a goroutine that checks for software interrupts and closes all kafka readers and writers when the main process ends.*/
func init() {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		kafkautils.CloseAllReaders()
		os.Exit(0)
	}()
}

/* Update metadata and write blender output artifact to minio */
func writeData(
	config helpers.Config,
	file_path string,
	metadata *metadata.Metadata) {
	// Try writing to minio
	minio_path := minioutils.WriteToMinioWithRetry(
		config.MinioEndpoint,
		config.Minio_Access_Key,
		config.Minio_Secret_Key,
		file_path,
		os.Getenv("SCANS_MINIO_BUCKET"),
	)
	log.Printf("File %s was written to MinIO", file_path)

	// update metadata object
	metadata.ScanFilehandle = minio_path
	metadata.ScanFiletype = ".ply"
	metadata.ScanCreationTimestamp = time.Now().Unix()
	// write metadata object to kafka
	kafkautils.WriteToKafkaWithRetry(config.Kafka_Brokers, os.Getenv("SCANS_TOPIC"), metadata)
	log.Printf("Metadata was written to Kafka")
	n, err := kafkautils.CommitMessages(
		config.Kafka_Brokers,
		os.Getenv("SCENE_QC_PASS_TOPIC"),
		"scenes-qc-pass-reader-group",
	)
	if err != nil {
		log.Printf(
			"Could not commit Offset. Offsets will be saved on the next successful commit. Otherwise this message will be reprocessed at a later time.",
		)
	}
	log.Printf("Committed %d Offsets.", n)

}

/* Create a Blender Job and forward it to the blender bootstrap component*/
func sendJobToBlender(
	inputStream *io.WriteCloser,
	inputFile string,
	outputFile string,
	workerSeed string,
	seed string,
) {
	job := fmt.Sprintf("JOB %s %s %s %s\n", inputFile, outputFile, workerSeed, seed)
	log.Printf("Forwarding this Job to the blender bootstrap: %s", job)
	io.WriteString(*inputStream, job)
}

/* Parse blender output into a BlenderEvent Struct */
func handleBlenderResponse(
	line string,
) BlenderEvent {
	split := strings.Split(line, " ")
	return BlenderEvent{
		kind:       "RESULT",
		outputFile: split[1],
		seed:       split[2]}
}

/* Consume Data from Kafka*/
func streamData(
	conf helpers.Config,
	stdin *io.WriteCloser,
	blenderChan chan BlenderEvent,
) {
	for { // repeat until the heat death of the universe
		// retrieve metadata from kafka
		metadata := kafkautils.ReadFromKafkaWithRetry[*metadata.Metadata](
			conf.Kafka_Brokers,
			os.Getenv("SCENE_QC_PASS_TOPIC"),
			"scenes-qc-pass-reader-group",
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
			os.Getenv("SCENE_QC_PASS_BUCKET"),
			scene_path,
		)

		// generate the worker seed for this specific stage from work item
		jobSeed, err := helpers.GenerateWorkerSeedForStage(
			metadata.Seed,
			os.Getenv("LIDAR_STAGE_SEED"),
		)
		if err != nil {
			log.Printf("could not get seed for this job: %v", err)
			continue
		}
		// add the job seed to the metadata inforation of this work item
		metadata.ScanSeed = jobSeed

		// compute the output file name
		output_path := "/output/" + strings.Split(filename, ".")[0] + "_scan_" + ".ply"
		err = os.MkdirAll("/output", 0755)
		if err != nil {
			log.Printf("Could not create output directory")
			continue
		}

		// create a blender job and pass it to the bootstrap service
		sendJobToBlender(stdin, scene_path, output_path, jobSeed, metadata.Seed)

		// wait on this channel for job completion
		// a logging goroutine continually scans all blender output. When a result line is found, it is written to this channel, signifying a work item completion from blender.
		ev := <-blenderChan

		// this check is technically not necessary but if we would want to send multiple result events to blender this would warn the user that we dont match the wrong metadata to the wrong event.
		if ev.seed != metadata.Seed {
			log.Printf(
				"Unexpected Result: seed: %s expected: %s. Ignoring...",
				ev.seed,
				metadata.Seed,
			)
			continue
		}
		// write artifact to minio and update metadata
		writeData(conf, ev.outputFile, metadata)
	}
}

// The main function that registers the directory watcher and launches the blender command
func main() {
	conf := helpers.ReadEnvs()
	// start a blender instance using the bootstrap script, providing the user script as an argument
	cmd := exec.Command(
		"blender",
		"--background",
		"--python",
		"/workspace/bootstrap.py",
		"--",
		"--f",
		"/workspace/lidar_scan.py", // user script
	)
	// set up stdout streaming for logging and results from the blender instance
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	merged := io.MultiReader(stdout, stderr)
	// set up stdin for passing jobs to the blender instance
	stdin, _ := cmd.StdinPipe()

	// create a channel that is used to signify waiting for a result completion
	blenderChan := make(chan BlenderEvent, 1)

	// Set up a goroutine that runs in the background and functions as logging passthrough and result handler for blender
	go func() {
		scanner := bufio.NewScanner(merged) // create a reader on the output of the blender instance
		for scanner.Scan() {                // read every blender output
			line := scanner.Text()
			if strings.HasPrefix(
				line,
				"RESULT",
			) { // if the output is a result line for a job, parse it and pass it through the channel
				blenderChan <- handleBlenderResponse(line)
			}
			fmt.Println("[BLENDER]", line) // otherwise, print Blender logs to global logs
		}
	}()

	defer stdout.Close()
	defer stderr.Close()

	// start the blender worker check if startup successful, otherwise fail
	if err := cmd.Start(); err != nil {
		log.Fatalf("Blender exited with error:%v", err)
	}

	streamData(conf, &stdin, blenderChan) // start data consumption from kafka

	if err := cmd.Wait(); err != nil {
		log.Fatalf("Blender exited with error:%v", err)
	}
}
