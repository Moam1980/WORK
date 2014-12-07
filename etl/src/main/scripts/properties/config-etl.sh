#!/bin/bash

# General properties to generate CSV files
LINE_SIZE=32000

# Output directories
OUTPUT_FILE_PATH=/Users/vmrh/Mobily-Data
ISOP_OUTPUT_FILE_PATH=${OUTPUT_FILE_PATH}/isop

# Log directory
LOG_PATH=${OUTPUT_FILE_PATH}/isop/logs
ISOP_LOG_PATH=${LOG_PATH}/download

# Log files
ISOP_DOWNLOAD_LOG_FILE=${ISOP_LOG_PATH}/download

# Number of download process to run in parallel
ISOP_DOWNLOAD_PARALLEL_PROCS=5


# Properties to connect to database
DB_CONNECT_STRING="ISOP"
DB_USER="Isop"
DB_PASSWORD="isop"

