# Synphony
This repository contains the source code and documentation for the configurable blender pipeline synphony, which can be used to generate synthetic lidar data or similar workflows on distributed compute clusters running kubernetes.
The repository provides capabilities for running the project on a local docker-compose instance as well as configs to deploy the cluster to kubernetes (or KinD).


The system was tested using the fixed version specified in the Dockerfiles of the different services as well as the library versions specified in go.mod and go.sum
The only exception was the blainder-range-scanner blender addon used for the virtual laser scanning user script, as only one version for this addon is available.
A list of external docker digests is provided here:
golang:1.25.1-bookworm                          sha256:c423747fbd96fd8f0b1102d947f51f9b266060217478e5f9bf86f145969562ee
pdal/pdal:sha-ccf7b43d-amd64                    sha256:654b011d055c7ef641619f51f37254cd2925aae336d1d4a8d74dd83c6fc2efb2
nytimes/blender:3.3.1-cpu-ubuntu18.04           sha256:a9def1b911a226c027a3c2c7fabb0d46cf088aeb52b3b0b5be679083515670b7
alpine:3.23.3                                   sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

# Directories
## config
contains configuration files and documentation for a kubernetes deployment
## proto
contains the protobuf definitions of various message types used within the pipeline
## scripts
contains libraries and various helper scripts used in the development and testing of the pipeline
## services
contains the source code and relevant dockerfiles for each microservice within the pipeline

# Files
## .env
specifies default configuration for a test pipeliner run
## docker-compose.yml
specifies local docker-compose deployment configuration
## Dockerfile.gobuilder
base image for every service of the pipeline,
Builds every go script within the pipeline, to ensure that that every script is ran using the same configuration, services and libraries
Must be manually built and tagged properly before the pipeline can be built using the Makefile
build:
- "make build" builds the entire pipeline container structure starting with the go builder on linux and windows systems with make capabilities
- the builder script can be built and tagged manually using this command: "docker build -t go-builder:1.0 -f ./Dockerfile.gobuilder .
- all other docker containers can be built using docker-compose build for a manual build
## go.mod go.sum
contain information about the go project and its dependencies, is needed by the go-builder to download necessary libraries for the build process
## Makefile
contains information about how to build the pipeline
can be ran using make build to build the entire pipeline container structure from scratch
