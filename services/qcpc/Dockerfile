# Build the go binary that is the orchestrator and entrypoint for this worker
FROM go-builder:1.0 AS builder
COPY ./scripts/worker.go /workspace/worker.go
WORKDIR /workspace
RUN  go mod tidy && go build -o "worker" .
RUN chmod +x worker
#Base Image
FROM pdal/pdal:sha-ccf7b43d-amd64

WORKDIR /workspace

#copy the necessary scripts from the script directory into the container
COPY ./scripts/* ./ 
# set up worker binary
COPY --from=builder /workspace/worker ./worker

ENTRYPOINT /bin/bash -c "/workspace/worker" 
