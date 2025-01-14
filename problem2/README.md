# Data Engineering Solution

This project represents solutions to two problem statements:
- Fixed-width parser
- Spark-anonymizer

## Fixed-Width Parser

This project provides a Python application to parse fixed-width files into CSV files based on a given specification. The application includes functionality to read a specification file, parse a fixed-width input file, and output the results to a CSV file.

## Spark Anonymization Project

This project provides a Dockerized Spark application to anonymize specified columns in a dataset. The application can handle small, medium, and large datasets, and it supports CSV and Parquet file formats.

## Requirements

- Python 3.9+
- PySpark
- `pip` (Python package installer)
- Docker (optional, for containerized execution)
- Docker Desktop with WSL2 (for Windows users)
- Make

## Setup

### Docker Setup (For Spark Anonymizer)

1. **Install Docker**:
   - Follow the instructions on the [Docker website](https://www.docker.com/products/docker-desktop) to install Docker Desktop.

2. **Ensure WSL2 is Installed and Configured** (for Windows users):
   - Install WSL2 following [Microsoft's instructions](https://docs.microsoft.com/en-us/windows/wsl/install).
   - Set WSL2 as the default backend for Docker in Docker Desktop settings.

3. **Ensure Docker Desktop is Running**:
   - Start Docker Desktop and ensure it is running properly.

### General Setup

4. **Ensure PySpark is Installed**:
   - Install PySpark using pip:
     ```bash
     pip install pyspark
     ```

5. **Install Make**:
   - Make is typically pre-installed on Linux and macOS. For Windows, you can install Make using Chocolatey:
     ```bash
     choco install make
     ```

## Usage

### Fixed-Width Parser

1. Prepare your specification file (e.g., `spec.txt`):

    ```
    Name:10
    Age:3
    City:15
    ```

2. Prepare your input fixed-width file (e.g., `input.txt`):

    ```
    John      25 New York       
    Jane      30 San Francisco 
    ```

3. Run the Fixed-Width Parser:

    ```bash
    make parse
    ```

4. The output CSV file will be generated at `fixed_width_parser/output.csv`.

### Spark Anonymizer

1. Prepare your input file (e.g., `input.csv`) depending on size (small, medium, large) in the format:

    ```
    first_name,last_name,DOB,Address
    john,prakash,1998-02-01,New York
    jay,Raj,1996-01-01,Miami
    ```

2. Build the Docker image:

    ```bash
    make build
    ```

3. Run the Docker container with default arguments:

    ```bash
    make run
    ```

4. Run the Docker container with custom arguments:

    ```bash
    make run SIZE=large TYPE=csv INPUT_PATH=/path/to/input.csv OUTPUT_PATH=/path/to/output COLS="first_name last_name"
    ```

5. The anonymized dataset will be saved at the specified output path.

### Cleaning Up

To remove the Docker image for the Spark Anonymizer:

```bash
make clean