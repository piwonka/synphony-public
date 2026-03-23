# Config
This directory contains deployment charts for a kubernetes deployment of the pipeline.
## Overview
### Kafka
Contains charts for the deployment of a Kafka cluster using strimzi and Kafka in Kraft mode
### minio
contains a script that can be used to deploy MinIO with a sample configuration on a kubernetes cluster using helm
### services
contains the deployment charts for all services of the pipeline, as well as a pvc and a job for the injection of seeds into the pipeline

# Local Cluster Setup
## Prerequisites for a local cluster setup:
KinD
helm
kubectl
## Setup:
The following steps are a broad pointer on how to set up a local kind cluster for testing the pipeline on linux. It is assumed that these commands are ran from the config directory, paths may need to be adjusted otherwise.
### Create Cluster
kind create cluster --config=kind.yaml
### Helm
helm repo add strimzi https://strimzi.io/charts
helm repo add minio https://charts.min.io/
helm repo update

### Apply Local Path Storage Provisioner
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml

### Kafka
kubectl create namespace kafka
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --namespace kafka  --set watchNamespaces={kafka}
kubectl apply -n kafka -f kafka/kafka-cluster.yaml

Wait until the cluster is properly established
check with kubectl -n kafka get pods -w

Set the bootstrap value of the create-topics job using information from the command:
kubectl get svc -n kafka

deploy job
kubectl -n kafka apply -f kafka/create-topics.yaml

### MinIO
kubectl create namespace minio

create minio deployment using this script:
./minio/create_minio_deployment.sh

### Services
build and tag every service on your local machine using "docker build -t <tag> ." for each service

load each docker image from the local repository onto the KinD cluster using:
kind load docker-image <service:tag> --name synphony-cluster

This may take a lot of time as every image needs to be distributed between the workers and the blender image is rather large

kubectl create namespace synphony

kubectl apply -f ./config-map.yaml

then deploy every service (except the seed job) using this command:
kubectl -n synphony apply -f services/<deployment>

scale the deployment as needed:
kubectl -n synphony scale deployment --all --replicas=X

deploy the seed job:
1. ensure the seed state can be stored persistently, otherwise the service will not run
kubectl -n synphony apply -f seeds-claim0-persistent-volume-claim.yaml 
2. deploy the service:
kubectl -n synphony apply -f seeds-job.yaml


The pipeline now runs if everything is set up correctly
Pipeline state can be checked with:
kubectl -n synphony get pods -w -> shows all pods and their states
kubectl -n synphony logs -l -w app="<servicename>" -> shows combined logs of all replicas from a service 
kubectl -n namespace exec --stdin --tty podname -- /bin/bash -> starts tty within a specified pod

check data in minio:
1. tty in minio
in minio tty:
mc alias set localminio http://localhost:9000 minioadmin minioadmin123
now data can be checked and modified using mc
example:
mc ls localminio/<bucketname> -> lists all files within a bucket

for further information check the kubectl and mc documentation here:
https://minio.github.io/mc/
https://kubernetes.io/docs/reference/kubectl/
