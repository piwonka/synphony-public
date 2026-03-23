# Build the go binary that is the orchestrator and entrypoint for this worker
FROM go-builder:1.0 AS builder
COPY ./scripts/worker.go /workspace/worker.go
WORKDIR /workspace
RUN  go mod tidy && go build -o "worker" .
RUN chmod +x worker
#Base Image
FROM nytimes/blender:3.3.1-cpu-ubuntu18.04
ARG BLENDER_VERSION=3.3
#Blender Version
ENV BLENDER_VERSION=${BLENDER_VERSION}
#Set up AddOn Path
ENV BLENDER_USER_SCRIPTS="/root/.config/blender/$BLENDER_VERSION/scripts"
RUN mkdir -p $BLENDER_USER_SCRIPTS/addons
WORKDIR $BLENDER_USER_SCRIPTS/addons
#Setup Environment for AddOn Installation
RUN apt update && apt install -y git python3 python3-pip dos2unix &&\
	#Download AddOn, Install Requirements, Enable it
	git clone https://github.com/ln-12/blainder-range-scanner.git/ ./temp &&\
	mv ./temp/range_scanner ./range_scanner && rm -r ./temp &&\
	pip install h5py==3.12.1 laspy==2.5.4 open3d==0.18.0 pascal-voc-writer==0.1.4 pypng==0.20220715.0 PyYAML==6.0.2 &&\
	blender -b --python-expr "import bpy; bpy.ops.preferences.addon_enable(module='range_scanner'); bpy.ops.wm.save_userpref(); print([a.module for a in bpy.context.preferences.addons])"

WORKDIR /workspace
# Set up scene generator
COPY ./scripts/generate_scenes.py ./generate_scenes.py
COPY ./scripts/bootstrap.py ./bootstrap.py
# set up worker binary
COPY --from=builder /workspace/worker ./worker

ENTRYPOINT /bin/bash -c "/workspace/worker"
