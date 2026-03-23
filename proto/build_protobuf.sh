#!/bin/sh
# This has to be executed from the level above the protobuf files and will build all of them

# Base directory
BASE_DIR="$(pwd)"

# Find all directories containing .proto files
find "$BASE_DIR" -type f -name "*.proto" -exec dirname {} \; | sort -u | while read -r dir; do
    echo "Generating Go protobufs in $dir..."
    for proto_file in "$dir"/*.proto; do
        echo "  -> Processing $proto_file"
        protoc --proto_path="$dir" --go_out="$dir" --go_opt=paths=source_relative "$proto_file"
    done
done

echo "All protobuf files generated successfully."

