package minioutils

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var cachedClient *minio.Client // store client and reuse it

// define some globals for retry mechanisms
var minDelay = 200 * time.Millisecond
var maxDelay = 10 * time.Second
var jitter = 100 * time.Millisecond

/*Apply the function fn and return a tuple of T, error. Retry until a result is returned with varying backoff durations.*/
func retryWithResult[T any](fn func() (T, error)) T {
	var result T
	var err error
	sleep := minDelay
	for {
		result, err = fn()
		if err == nil { // invoke function, if result is present, return the result
			return result
		}
		// otherwise back off
		log.Printf("Minio Access Failed. Backing Off before Retrying due to Error: %v", err)
		j := time.Duration(rand.Int63n(int64(jitter)*2+1)) - jitter
		sleep += j
		if sleep <= minDelay || sleep >= maxDelay {
			sleep = minDelay
		}
		time.Sleep(sleep)
	}
}

/*Create the Minio Client for a specific endpoint using the credentials provided. Retry with varying backoff intervals. */
func CreateClientWithRetry(endpoint string, access_key string, secret_key string) {
	retryWithResult(
		func() (struct{}, error) { // use struct as a placeholder for the type T in the generic function signature, as we do not need a return value
			return struct{}{}, createClient(endpoint, access_key, secret_key)
		},
	)
}

/* Write a file stored locally at file_path to a specific minio bucket using the credentials provided, with varying retry intervals.
*  Return the minio path the file is stored at.*/
func WriteToMinioWithRetry(
	endpoint string,
	access_key string,
	secret_key string,
	file_path string,
	bucket string,
) string {
	return retryWithResult(
		func() (string, error) {
			return writeToMinio(endpoint, access_key, secret_key, file_path, bucket)
		},
	)
}

/* Retrieve a file at endpoint/bucket/file from minio and write it to the local result_path. Retry with varying backoff duration */
func ReadFromMinioWithRetry(
	endpoint string,
	access_key string,
	secret_key string,
	file string,
	bucket string,
	result_path string,
) {
	retryWithResult(
		func() (struct{}, error) {
			return struct{}{}, readFromMinio( // use struct as a placeholder for type T for the generic function signature, as we do not need T
				endpoint,
				access_key,
				secret_key,
				file,
				bucket,
				result_path,
			)
		},
	)
}

/* Create a minio client for the specified endpoint using the provided credentials */
func createClient(endpoint string, access_key string, secret_key string) error {
	c, err := minio.New(
		endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(access_key, secret_key, ""),
			Secure: false, // use http
		},
	)
	if err != nil {
		log.Printf("error creating MinIO client: %v", err)
		return err
	}
	cachedClient = c
	log.Println("Client creation Successful")
	return nil
}

/*Write a file from the local file_path to the specified minio bucket at endpoint/bucket/fileName. Returns the minio storage path and potentials errors as a tuple */
func writeToMinio(
	endpoint string,
	access_key string,
	secret_key string,
	file_path string,
	bucket string,
) (string, error) {
	ctx := context.Background()
	if cachedClient == nil { // ensure that a client exists before continuing
		CreateClientWithRetry(endpoint, access_key, secret_key)
	}

	exists, err := cachedClient.BucketExists(ctx, bucket) // Check if the bucket exists
	if err != nil {
		log.Printf("Bucket check failed %v", err)
		return "", err
	}
	if !exists { // if the bucket does not exist yet ( first time writing to it), create it.
		err = cachedClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			log.Printf("Bucket creation failed: %v", err)
			return "", err
		}
		log.Println("Bucket creation Successful")
	}

	// open a byte stream reader of the local file
	file, err := os.Open(file_path)
	if err != nil {
		log.Printf("error opening file %s", filepath.Base(file_path))
	}
	// retrieve file information
	info, err := file.Stat()
	if err != nil {
		log.Printf("Could not get File Information")
		return "", err
	}
	// write the file to minio
	_, err = cachedClient.PutObject(
		ctx,
		bucket,
		filepath.Base(file_path),
		file,
		info.Size(), // file size
		minio.PutObjectOptions{ContentType: "application/blender"}, // specify content type
	)
	minio_file_location := fmt.Sprintf(
		"s3://%s/%s",
		bucket,
		filepath.Base(file_path),
	) // build minio file storage location
	if err != nil {
		log.Printf("unable to put object %s: %v", minio_file_location, err)
		return "", err
	}
	return minio_file_location, nil
}

/* Retrieve a file from a spefic minio bucket and store it at the local result_path. Returns nil if retrieval was successful and an error otherwise*/
func readFromMinio(
	endpoint string,
	access_key string,
	secret_key string,
	file string,
	bucket string,
	result_path string,
) error {
	if cachedClient == nil { // ensure client exists before continuing
		CreateClientWithRetry(endpoint, access_key, secret_key)
	}
	ctx := context.Background()
	err := cachedClient.FGetObject(
		ctx,
		bucket,
		file,
		result_path,
		minio.GetObjectOptions{},
	) // write minio object straight to the local path provided
	if err != nil {
		log.Printf("error getting object %s/%s from minio:%v", bucket, file, err)
		return err

	}
	return nil
}
