# Services
Contains all necessary source code and Dockerfiles to build each pipeline service from scratch

## seeds
creates seed-based work ites
## scenes
creates .blend files from these work items
## qcsc (Quality Control Scenes)
performs quality control checks on the blend files from scenes
## lidar
performs virtual laser scanning on the blend files that passed qcsc
and creates pointclouds from them (default=.ply)
## qcpc
performs quality control checks on the point cloud creates in lidar
