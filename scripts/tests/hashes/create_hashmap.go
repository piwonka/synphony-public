package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/piwonka/synphony/proto/metadata"
	kafkautils "github.com/piwonka/synphony/scripts/lib/kafka"
	minioutils "github.com/piwonka/synphony/scripts/lib/minio"
)

func main() {
	numberOfSeeds, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Println("Could not parse number of Seeds")
		os.Exit(1)
	}
	kafka_topic := os.Args[2]
	kafka_brokers := strings.Split(os.Args[3], ",")
	kafka_group_id := "test-reader-" + uuid.NewString()
	minio_bucket := os.Args[4]
	minio_endpoint := os.Args[5]
	minio_access_key := os.Args[6]
	minio_secret_key := os.Args[7]

	hashes := make(map[string]string, 0)
	for range numberOfSeeds {
		log.Println("Reading Metadata from kafka")
		m := kafkautils.ReadFromKafkaWithRetry[*metadata.Metadata](
			kafka_brokers,
			kafka_topic,
			kafka_group_id,
		)
		// Copy the .bled file from minio to local disc
		filename := filepath.Base(
			m.ScanFilehandle,
		) // example for metadata.Filehandle: s3://bucket/file
		file_path := "./minio_data/" + filename
		log.Printf("Read metadata with id %s from kafka", m.Seed)
		minioutils.ReadFromMinioWithRetry(
			minio_endpoint,
			minio_access_key,
			minio_secret_key,
			filename,
			minio_bucket,
			file_path,
		)
		log.Printf("file %s was read and saved to %s", filename, file_path)
		f, err := os.Open(file_path)
		if err != nil {
			log.Fatal(err)
		}
		hasher := sha256.New()
		if _, err := io.Copy(hasher, f); err != nil {
			log.Fatal(err)
		}

		hashes[m.Seed] = fmt.Sprintf("%x", hasher.Sum(nil))

		err = f.Close()

		if err != nil {
			log.Fatalf("Could not close file")
		}
	}
	err = writeJsonL(hashes)
	if err != nil {
		log.Fatalf("Error writing JSONL %v", err)
	}
	log.Printf("Result written to %s", os.Args[8])
	// store the entire map in a json file

}

type Rec struct {
	Seed string `json:"seed"`
	Hash string `json:"hash"`
}

func writeJsonL(hashes map[string]string) error {
	f, err := os.Create("./results/" + os.Args[8] + ".jsonl")
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for s, h := range hashes {
		r := Rec{
			Seed: s,
			Hash: h,
		}
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}
