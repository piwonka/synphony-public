#!/bin/bash
ID="$1"
docker exec minio bash -c "\
	mc alias set local http://localhost:9000/ test testtest && \
	mc cp local/scenes/terrain_$ID.blend /tmp/terrain.blend && \
	mc cp local/scans/terrain_${ID}_scan__frames_1_to_1.ply /tmp/scan.ply "
docker cp minio:/tmp/terrain.blend ~/Desktop/Master/debug/terrain.blend
docker cp minio:/tmp/scan.ply ~/Desktop/Master/debug/scan.ply

/opt/blender-3.3/blender ~/Desktop/Master/debug/terrain.blend --python load_ply_view.p --python load_ply_view.py
