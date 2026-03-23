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
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v3"
)

/*Runs on startup, creates a goroutine that checks for software interrupts and closes all kafka readers when an interrupt is detected*/
func init() {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig // wait for interrupt
		kafkautils.CloseAllReaders()
		os.Exit(0)
	}()
}

/*Quality Control Specification, built from qc_spec.yaml and contains an array of all Quality Gates specified*/
type QCSpec struct {
	Gates []QualityGate `yaml:"quality_gates"`
}

/*Quality Control Specification, built from qc_spec.yaml, contains information about each specific quality gate*/
type QualityGate struct {
	Type         string   `yaml:"type"`          // string represents quality gate name
	Path         []string `yaml:"path"`          // json path of the checked value inside pdal output
	Op           string   `yaml:"op"`            // operation that is used to check the quality gate value
	ThresholdEnv string   `yaml:"threshold_env"` // the threshold that the checked value is compared against using Op
}

/*Reads Quality Control Specification from qc_spec.yaml, returns a struct containing the information of each quality gate specified or an error if the spec is faulty*/
func LoadQCSpec(path string) (*QCSpec, error) {
	file, err := os.ReadFile(path) // read the spec from file
	if err != nil {
		return nil, err
	}

	var spec QCSpec
	if err := yaml.Unmarshal(file, &spec); err != nil { // unmarshal the qc_spec.yaml file into the fields prepared within the quality gate structs
		return nil, err
	}
	return &spec, nil
}

/*Reads the JSON file generated from the PDAL execution and returns a byte[] of the file or an error*/
func LoadPDALJson(path string) ([]byte, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}

/*Builds a JSON path from the information provided within the QCSpec, returns the path*/
func SpecToGJSONPath(gateValuePath []string) string {
	path := ""
	for i, element := range gateValuePath { // iterate over array elements
		if i > 0 { // if current element is not the first add dot to connect the parts
			path += "."
		}

		// check if the element contains a dot and if it does, escape it
		if strings.Contains(element, ""+".") {
			escaped_element := ""
			for s := range strings.SplitSeq(element, ".") {
				escaped_element += s + "\\."
			}
			path += escaped_element[:len(escaped_element)-2] // remove final escaped dot
		} else {
			path += element
		}

	}
	return path

}

/*Uses the operation and threshold within the quality gate information to check wether the Point cloud passes this quality gate, returns a boolean or an error*/
func ComputeQualityGate(gate QualityGate, value float64, threshold float64) (bool, error) {
	switch gate.Op {
	case ">=":
		return value >= threshold, nil
	case "<=":
		return value <= threshold, nil
	case ">":
		return value > threshold, nil
	case "<":
		return value < threshold, nil
	default:
		return false, fmt.Errorf("unknown op %s", gate.Op)
	}

}

/*Computes all quality gate decisions presented in QCSpec for the PDAL pipeline result of a specific pointcloud, returns QCDecision array or an error */
func ApplySpecToJSON(spec QCSpec, pdalResult []byte) ([]*metadata.QCDecision, error) {
	qc := make(
		[]*metadata.QCDecision,
		0,
		len(spec.Gates),
	) //create an array of QC decisions equal to the number of QCDecisions neccessary
	for _, gate := range spec.Gates { // iterate over all quality gates
		path := SpecToGJSONPath(gate.Path)         // compute the pdal path for this gate
		result := gjson.GetBytes(pdalResult, path) // read the pdal result as bytes
		if !result.Exists() {                      // check if the pdal result is present, if not return an error
			return nil, fmt.Errorf("path not found %s", path)
		}
		val := result.Float() // transform result to a number
		threshold, err := strconv.ParseFloat(
			strings.TrimSpace(os.Getenv(gate.ThresholdEnv)),
			64,
		) // parse the threshold presented in the QCSpec for this gate
		if err != nil {
			return nil, fmt.Errorf("could not parse threshold env %s:%v", gate.ThresholdEnv, err)
		}
		gatePassed, err := ComputeQualityGate(
			gate,
			val,
			threshold,
		) // compute the quality gate using the threshold and ops provided in the QCSpec
		if err != nil {
			return nil, fmt.Errorf("error computing qualitygate %s", gate.Type)
		}
		// create a qc decision from the computed data and append it to the array
		qcDecision := &metadata.QCDecision{
			Type:              gate.Type,
			CreationTimestamp: time.Now().UnixMilli(),
			Decision:          gatePassed,
			Data:              strconv.FormatFloat(val, 'f', -1, 64),
		}
		qc = append(qc, qcDecision)
	}
	return qc, nil // return the array of qcdecisions
}

/* write the created artifact to minio, update the metadata and write it to kafka, commit completed work item*/
func WriteData(
	config helpers.Config,
	file_path string,
	seed string,
	metadata *metadata.Metadata) {

	// decide the target artifact bucket and kafka topic based on wether the point cloud passed the qc checks
	correct_bucket := ""
	correct_topic := ""
	if metadata.PassedQc {
		correct_bucket = os.Getenv("SCAN_QC_PASS_BUCKET")
		correct_topic = os.Getenv("SCAN_QC_PASS_TOPIC")
	} else {
		correct_bucket = os.Getenv("SCAN_QC_FAIL_BUCKET")
		correct_topic = os.Getenv("SCAN_QC_FAIL_TOPIC")
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

	// update metadata using the minio artifact storage location
	metadata.Filehandle = minio_path
	// write metadata object to kafka
	kafkautils.WriteToKafkaWithRetry(config.Kafka_Brokers, correct_topic, metadata)
	log.Printf("Metadata was written to Kafka")
	// commit the work item to signify its completion
	n, err := kafkautils.CommitMessages(
		config.Kafka_Brokers,
		os.Getenv("SCANS_TOPIC"),
		"scans-reader-group",
	)
	if err != nil {
		log.Printf(
			"Could not commit Offset. Offsets will be saved on the next successful commit. Otherwise this message will be reprocessed at a later time.",
		)
	}
	log.Printf("Committed %d Offsets.", n)
}

/* The main function that reads metadata from kafka, and computes qc decisions for all artifacts using PDAL*/
func main() {
	// read envs using config helpers
	conf := helpers.ReadEnvs()
	spec, err := LoadQCSpec("qc_spec.yaml") // load qc spec
	if err != nil {
		log.Fatalf("Could not Load Spec for Quality Gate Parameters! %v", err)
	}
	for { // repeat until heat death of the universe
		//retrieve metadata from kafka
		job_metadata := kafkautils.ReadFromKafkaWithRetry[*metadata.Metadata](
			conf.Kafka_Brokers,
			os.Getenv("SCANS_TOPIC"),
			"scans-reader-group",
		)

		// Copy the pointcloud file from minio to local disc
		filename := filepath.Base(
			job_metadata.ScanFilehandle,
		) // example for metadata.Filehandle: s3://bucket/file
		pointcloud_path := "/tmp/minio/" + filename
		minioutils.ReadFromMinioWithRetry(
			conf.MinioEndpoint,
			conf.Minio_Access_Key,
			conf.Minio_Secret_Key,
			filename,
			os.Getenv("SCANS_MINIO_BUCKET"),
			pointcloud_path,
		)

		// prepare pdal pipeline file injection ( replace placeholder within the pipeline with actual file that the computation is ran on)
		dynamicFileReaderOption := fmt.Sprintf(
			"--readers.%s.filename=%s",
			job_metadata.ScanFiletype[1:],
			pointcloud_path,
		)

		resultFileName := fmt.Sprintf("result_%s.json", job_metadata.Seed)
		// invoke PDAL pipeline, injecting the current file
		cmd := exec.Command(
			"pdal",
			"pipeline",
			"pipeline.json",
			dynamicFileReaderOption,
			"--metadata",
			resultFileName,
		)
		log.Printf("Running command %s", cmd.String())
		// forward PDAL outputs to logs, output is autoclosed on pdal run end
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()
		merged := io.MultiReader(stdout, stderr)

		// Set up logging passthrough
		go func() {
			scanner := bufio.NewScanner(merged)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Println("[PDAL]", line) // prints PDAL logs
			}
		}()

		// start PDAL check if startup successful
		if err := cmd.Start(); err != nil {
			log.Printf("PDAL exited with error:%v", err)
		}
		// wait for PDAL completion
		if err := cmd.Wait(); err != nil {
			log.Printf("PDAL exited with error:%v", err)
		}
		// load json result
		jsonBytes, err := LoadPDALJson(resultFileName)
		if err != nil {
			log.Printf("could not load JSON %s:%v", resultFileName, err)
			continue
		}
		// compute QCDecisions for pdal output
		qcObjects, err := ApplySpecToJSON(*spec, jsonBytes)

		if err != nil {
			log.Printf("could not compute qualityGates: %v", err)
			continue
		}
		// update metadata with qcObjects
		job_metadata.Qc = qcObjects
		// collect qc decisions to check if qc is passed
		for _, qc := range job_metadata.Qc {
			// no need to initialize this value, scene qc will already have initialized this
			job_metadata.PassedQc = job_metadata.PassedQc && qc.Decision
		}
		// write the new data
		WriteData(conf, pointcloud_path, job_metadata.Seed, job_metadata)
	}
}
