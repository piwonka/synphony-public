#!/bin/bash
# execute like this: docker exec -i kafka bash -c 'BOOTSTRAP=kafka:19092 PARTITIONS=3 REPLICATION=1 MIN_ISR=1 bash -s' < create-topics.sh
set -euo pipefail # exit on any failure, skip all following lines on fail

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"
MIN_SYNCED_REPLICAS="${MIN_SYNCED_REPLICAS:-1}"
TOPICS=(
	seeds
	scenes
	scans
	scenes-pass
	scenes-fail
	scans-pass
	scans-fail
)

create_topic () {
	local t="$1"
	/opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--topic "$t" \
		--bootstrap-server "$BOOTSTRAP" \
		--partitions "$PARTITIONS" \
		--replication-factor "$REPLICATION" \
		--config min.insync.replicas="$MIN_SYNCED_REPLICAS"
}

for t in "${TOPICS[@]}"; do
	echo "Creating topic: $t"
	create_topic $t
done

echo "Done creating Topics:"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP"
