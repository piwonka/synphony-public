#VARIABLES
BUILDER_IMAGE := go-builder:1.0

#Build the Builder Image
.PHONY: build-base
build-base:
	@echo "Building Go-Builder image..."
	-@docker rmi -f $(BUILDER_IMAGE) 2>/dev/null||true # remove the old tag so that libs are correct
	docker build -t $(BUILDER_IMAGE) -f ./Dockerfile.gobuilder .

# Build all services that are derived from the base image
.PHONY: build-services
build-services:
	@echo "Building Services..."
	docker-compose build seeds scenes lidar qcsc qcpc
# BUILD ALL (MAKE BUILD)
.PHONY: build
build: build-base build-services
	@echo "ALL Builds complete."
