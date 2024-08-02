# Docker image name
IMAGE_NAME = Fixed-width-parser

# Default values for the arguments (you can override these when running make)
SPEC ?= /path/to/specfile.csv
INPUT_PATH ?= /path/to/input.csv
OUTPUT_PATH ?= /path/to/output

# Target to build the Docker image
.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

# Target to run the Docker container
.PHONY: run
run: build
	docker run --rm -v $(PWD):app/problem1/src/main.py $(IMAGE_NAME) --specfile $(SPEC) --input_path $(INPUT_PATH) --output_path $(OUTPUT_PATH)

# Clean target to remove Docker images (optional)
.PHONY: clean
clean:
	docker rmi -f $(IMAGE_NAME)
