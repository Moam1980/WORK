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
DB_CONNECT_STRING="IMS"
DB_USER="ims_cell"
DB_PASSWORD="imscell"

# Configuration for hadoop file size output
HADOOP_OUTPUT_FILE_PATH=${OUTPUT_FILE_PATH}/hadoop
HADOOP_PATH=hdfs://10.64.247.224/user/tdatuser/
HADOOP_FILES_SIZE_FILE=/data/landing/cs-probes/hadoop_files_summary.csv

# CS hadoop configuration
HADOOP_CS_PROBES_OUTPUT_FILE_PATH=${HADOOP_OUTPUT_FILE_PATH}/cs-probes

# CS source configuration
CS_USER="edm"
CS_SERVER="10.64.9.35"
CS_LANDING_AREA="/data/landing/cs-probes"
CS_DIRECTORIES=("A-Interface" "IUCS-Interface")
CS_DATA_SEPARATOR=("" ".")
CS_FILE_PREFIXES=("ADR_AINT_6-" "TDR-IU-7-")
CS_FILE_EXTENSIONS=(".gz" ".gz")
