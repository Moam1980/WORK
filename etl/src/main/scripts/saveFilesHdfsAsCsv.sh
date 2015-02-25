#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -v <edm_version> -o <spark_options> \
        -p <properties_file> -f <override_flag>"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -v <edmVersion>: Version of EDM core to use, is mandatory"
    echo 1>&2 "    -o <spark_options>: Spark options to run process, is mandatory"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is mandatory"
    echo 1>&2 "    -f <override_flag>: Flag to override folder if exists, is mandatory"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -v 0.8.0 -o \"--master yarn-client --executor-memory 1g\" \
        -p \"properties/etl-config.properties\" -f \"false\"
     -- Save as CSV all files in HDFS as master server"
}

# Check if number of parameters is the expected
if [ $# -eq 8 ]; then
    while getopts p:v:o:f: o
    do  case "${o}" in
        p)  propertiesFile="${OPTARG}";;
        v)  edmVersion="${OPTARG}";;
        o)  sparkOptions="${OPTARG}";;
        f)  overrideFlag="${OPTARG}";;
        [?])  echo 1>&2 "ERROR: ${0}:";usageHelp $*;exit 1;;
        esac
    done
else
    # Incorrect number of parameters
    echo 1>&2 "ERROR: ${0}: Number of parameters not correct: $#"
    usageHelp $*
    exit 2
fi

# Check propertiesFile parameter
if [ -z "${propertiesFile}" ]; then
    echo 1>&2 "ERROR: ${0}: Properties file is mandatory"
    usageHelp $*
    exit 5
fi

# Check EDM version parameter
if [ -z "${edmVersion}" ]; then
    echo 1>&2 "ERROR: ${0}: EDM core version is mandatory"
    usageHelp $*
    exit 6
fi

# Check overrideFlag parameter
if [ -z "${overrideFlag}" ]; then
    echo 1>&2 "ERROR: ${0}: Override flag is mandatory"
    usageHelp $*
    exit 7
fi

# Check overrideFlag parameter
if [ -z "${sparkOptions}" ]; then
    echo 1>&2 "ERROR: ${0}: Spark options are mandatory"
    usageHelp $*
    exit 8 
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh
SCALA_DIR="${BASE_DIR}/scala"
FILE="saveFilesHdfsAsCsv.scala"

# Load properties file
loadPropertiesFile "${propertiesFile}"

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    propertiesFile: ${propertiesFile}"
echo 1>&2 "    edmVersion: ${edmVersion}"
echo 1>&2 "    sparkOptions: ${sparkOptions}"
echo 1>&2 "    overrideFlag: ${overrideFlag}"

#Get a timestamp to generate SQL file
TIMESTAMP=`date +%s`
TMP_FILE="saveFilesHdfsAsCsv.tmp_${TIMESTAMP}.scala"
DEST_PATH="${HDFS_FILES_METRICS_PATH}"

echo 1<&2 "INFO: ${0}: Testing if destination directory exists: "
echo 1>&2 "    DEST_PATH: ${DEST_PATH}"
testAndDeleteInvalidParquetFolder ${DEST_PATH}
if [ $? == 0 ]; then
    echo "The sourceDirectory already exists."
    if [ ${overrideFlag} == "true" ]; then
        echo "Override flag is set to true. Deleting directory: ${DEST_PATH}"
        hdfs dfs -rm -r ${DEST_PATH}
    else
        echo "Skipping: ${DEST_PATH}"
        exit 0
    fi
fi

# Change template as required
echo 1<&2 "INFO: ${0}: Preparing scala code from template"
sed -e "s:\${HDFS_HOME}:${HDFS_HOME}:" -e "s:\${HDFS_FILES_METRICS_PATH}:${DEST_PATH}:" \
       ${SCALA_DIR}/${FILE} > ${SCALA_DIR}/${TMP_FILE}

${BASE_DIR}/runSpark.sh -v "${edmVersion}" -p "${propertiesFile}" -o "${sparkOptions}" -f "${SCALA_DIR}/${TMP_FILE}"
if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save Hdfs files to csv"
    echo 1<&2 "ERROR: ${0}: Code executed: "
    cat "${SCALA_DIR}/${TMP_FILE}"
fi
testAndDeleteInvalidParquetFolder ${DEST_PATH}

if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save Hdfs files to csv"
fi

# Cleaning
rm ${SCALA_DIR}/${TMP_FILE}

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process save as Hdfs files to csv, started at: ${startDate}, finished at: ${endDate}"
