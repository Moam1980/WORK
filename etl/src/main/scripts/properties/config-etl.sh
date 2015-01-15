#!/bin/bash

# General properties to generate CSV files
LINE_SIZE=32000

# Output directories
OUTPUT_FILE_PATH=/data/landing
BULK_DOWNLOAD_OUTPUT_FILE_PATH=${OUTPUT_FILE_PATH}/bulk-download

# Log directory
LOG_PATH=${OUTPUT_FILE_PATH}/isop/logs
ISOP_LOG_PATH=${LOG_PATH}/download

# Log files
ISOP_DOWNLOAD_LOG_FILE=${ISOP_LOG_PATH}/download

# Number of download process to run in parallel
ISOP_DOWNLOAD_PARALLEL_PROCS=5

# Properties to connect to ISOP database
DB_CONNECT_STRING="ISOP"
DB_USER="Isop"
DB_PASSWORD="isop"

# Configuration for hadoop file size output
HADOOP_PATH=/user/tdatuser

# CS hadoop configuration
HADOOP_CS_PROBES_FILE_PATH=${HADOOP_PATH}/cs-probes
HADOOP_CS_PROBES_VERSION=0.3
HADOOP_CS_PROBES_FORMAT=csv

# CS source configuration
CS_USER="edm"
CS_SERVER="10.64.9.35"
CS_LANDING_AREA="/data/landing/cs-probes"
CS_DIRECTORIES=("A-Interface" "IUCS-Interface")
CS_DATA_SEPARATOR=("" ".")
CS_FILE_PREFIXES=("ADR_AINT_6-" "TDR-IU-7-")
CS_FILE_EXTENSIONS=(".gz" ".gz")
