# Metadata
this directory contains protobuf definitions for the metadata object that is used within the pipeline. 

# Seeds
this directory contains protobuf definitions for the seed message object that is used by the seed service to inject work items into the pipeline

# build_protobuf.sh
this script generates a golang struct for the protobuf subdirectories and can be used locally.
it is needed by the gobuilder docker container to generate the go types for the kafka messages so that they are present as libs when the services are built locally.
