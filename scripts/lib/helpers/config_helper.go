package helpers

import (
	"os"
	"strings"
)

type Config struct {
	MinioEndpoint    string
	Minio_Access_Key string
	Minio_Secret_Key string
	Kafka_Brokers    []string
}

func ReadEnvs() Config {
	// read inputs and set up kafka seed reading parameters
	conf := Config{
		MinioEndpoint:    os.Getenv("MINIO_ENDPOINT"),
		Minio_Access_Key: os.Getenv("MINIO_ACCESS_KEY"),
		Minio_Secret_Key: os.Getenv("MINIO_SECRET_KEY"),
		Kafka_Brokers:    strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
	}
	return conf
}
