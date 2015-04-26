#!/bin/bash

# General properties to generate CSV files
LINE_SIZE=32000

# Output directories
OUTPUT_FILE_PATH=/data/landing
BULK_DOWNLOAD_OUTPUT_FILE_PATH=${OUTPUT_FILE_PATH}/bulk-download

# Log directory
LOG_PATH=${OUTPUT_FILE_PATH}/bulk-download/logs

# Log files
DOWNLOAD_LOG_FILE=${LOG_PATH}/download

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
HADOOP_CS_PROBES_PARQUET_FORMAT=parquet

# ISOP hadoop configuration -- Internet Analytics
HADOOP_ISOP_FILE_PATH=${HADOOP_PATH}/ia
HADOOP_ISOP_VERSION=0.3
HADOOP_ISOP_FORMAT=csv

# IMS hadoop configuration -- Database including cells
HADOOP_IMS_FILE_PATH=${HADOOP_PATH}/ims
HADOOP_IMS_VERSION=0.7
HADOOP_IMS_FORMAT=csv

# Oracle DWH hadoop configuration -- Database including subscribers
HADOOP_ORACLE_DWH_FILE_PATH=${HADOOP_PATH}/subscribers-oracle
HADOOP_ORACLE_DWH_VERSION=0.7
HADOOP_ORACLE_DWH_FORMAT=csv

# CS source configuration
CS_USER="edm"
CS_SERVER="10.64.9.35"
CS_LANDING_AREA="/data/landing/cs-probes"
CS_DIRECTORIES=("A-Interface" "IUCS-Interface")
CS_DATA_SEPARATOR=("" ".")
CS_FILE_PREFIXES=("ADR_AINT_6-" "TDR-IU-7-")
CS_FILE_EXTENSIONS=(".gz" ".gz")

# HDFS paths
HDFS_HOME="/user/tdatuser"
EVENTS_PARQUET_DIR="${HDFS_HOME}/cs-probes/event"
CELL_CATALOGUE="${HDFS_HOME}/cell-catalogue/0.7/2014/10/29/csv"
SUBSCRIBERS_FILE="${HDFS_HOME}/subscribers-oracle/0.9/2014/12/01/parquet"
USER_CENTRIC_DIR="${HDFS_HOME}/user-centric"
HDFS_FILES_METRICS_PATH="${HDFS_HOME}/metrics/hadoop-files/csv"
USER_ACTIVITY_DIR="${HDFS_HOME}/user-activity"

# Metrics configuration
METRICS_DIR="${HDFS_HOME}/metrics"
METRICS_FORMAT="csv"

SPARK_ARTIFACTS_DIR="/data/local-staging/tdatuser/artifacts"
