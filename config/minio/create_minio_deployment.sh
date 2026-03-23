#!/bin/bash
#the command in this script can be manually ran on windows for the same effect


helm install minio minio/minio -n minio \
--set mode=distributed \
--set replicas=4 \
--set persistence.enabled=true \
--set persistence.storageClass=standard \
--set persistence.size=10Gi \
--set rootUser=minioadmin \
--set rootPassword='minioadmin123' \
--set provisioning.enabled=false \
--set resources.requests.memory=128Mi \
--set resources.requests.cpu=50m \
--set resources.limits.memory=512Mi \
--set resources.limits.cpu=500m \
--timeout 10m
