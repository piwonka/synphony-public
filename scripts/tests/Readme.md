# Tests
This test directory contains the scripts used to retrieve some of the result critical information detailed in the Thesis.

Seeds:
- used to retrieve first and last created seed timestamps
Hashes:
- used to retrieve all artifacts from minio and link them do metadata from kafka, then compute a jsonl containing a map from seeds to artifact file hashes for run comparison
Compare:
- used to compare the jsonl results computed through hashes, gives information about missing seeds or different hashes per seed between runs

Binaries for these scripts are provided within the directories as well as their source code.
However, as these scripts were ran on the host during the tests, replicating the script usage requires port forwarding for minio and all kafka brokers.
Alteratively, they can be ran on a kubernetes cluster by deploying them as a pod.


As these scripts are not part of the main pipeline and therefore not really part of the thesis product, i will not document them in detail.
The usage of the scripts can be derived from the console arguments within the code.



# Misc
This directory contains scripts used for debugging the cluster on a local docker-compose run.
debug_minio.sh downloads scene and pointcloud artifacts for a specific seed ( usage: debug_minio.sh <seed>) and opens them directly in blender
This script may need to be modified to match the blender install on your system and the output directory of your choice.

init_kafka.sh is used to create the kafka topics for a local synphony cluster with the default topic configuration
