#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0}  -s <startDate> -e <endDate> -v <edm_version> -o <spark_options> \
        -p <properties_file> -f <override_flag>"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -s <startDate>: Start date for the command, is mandatory"
    echo 1>&2 "    -e <endDate>: End date for the command, is mandatory"
    echo 1>&2 "    -v <edmVersion>: Version of EDM core to use, is mandatory"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is mandatory"
    echo 1>&2 "    -f <override_flag>: Flag to override folder if exists, is mandatory"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0}  -s 20141004 -e 20141205 -v 0.8.0 -o \"--master yarn-client --executor-memory 1g\" \
        -p \"properties/etl-config.properties\" -f \"false\"
     -- Convert and save to Parquet data files from 20141004 to 20141205 using 10.64.246.168 as master server"
}

# Check if number of parameters is the expected
if [ $# -eq 12 ]; then
    while getopts s:e:p:v:o:f: o
    do  case "${o}" in
        s)  startDate="${OPTARG}";;
        e)  endDate="${OPTARG}";;
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

# Check start and end dates
if [ -z "${startDate}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Start date is mandatory: please include \"-s <startDate>\" as a parameter"
    usageHelp $*
    exit 3
fi

if [ -z "${endDate}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: End date is mandatory: please include \"-e <endDate>\" as a parameter"
    usageHelp $*
    exit 4
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
FILE="saveAsParquetFromEvent.scala"

# Load properties file
loadPropertiesFile "${propertiesFile}"

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    startDate: ${startDate}"
echo 1>&2 "    endDate: ${endDate}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"
echo 1>&2 "    edmVersion: ${edmVersion}"
echo 1>&2 "    sparkOptions: ${sparkOptions}"
echo 1>&2 "    overrideFlag: ${overrideFlag}"

# This script should run from startDate to endDate
# convert in seconds sinch the epoch:
start=$(date -d"$startDate" +%s)
end=$(date -d"$endDate" +%s)
cur=$start

# Run for all days in period
while [ $cur -le $end ]; do
    # Get date
    extractYearMonthDayFromEpoc ${cur}
    yearToPush=$yearExtracted
    monthToPush=$monthExtracted
    dayToPush=$dayExtracted
    sources=""
    count=0
    while [ ${count} -lt ${#CS_DIRECTORIES[@]} ]
    do
        csDirectory=${CS_DIRECTORIES[${count}]}
        dateSeparator=${CS_DATA_SEPARATOR[${count}]}
        datePath="${yearToPush}/${monthToPush}/${dayToPush}"
        basePath="${HADOOP_CS_PROBES_FILE_PATH}/${csDirectory}/${HADOOP_CS_PROBES_VERSION}/${datePath}"
        count=$((count + 1))
        if [ ! -z "${sources}" ]; then
            sources="${sources}, "
        fi
        sources="${sources}\"${basePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}\""
        # Increment one the counter
    done
    DEST_PATH="${EVENTS_PARQUET_DIR}/${HADOOP_CS_PROBES_VERSION}/${datePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
    #Get a timestamp to generate SQL file
    TIMESTAMP=`date +%s`
    TMP_FILE="saveAsParquetFromEvent.tmp_${TIMESTAMP}.scala"
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

    echo 1<&2 "INFO: ${0}: Preparing scala code from template" 
    sed -e "s:\${sources}:${sources}:" -e "s:\${destination_dir}:${DEST_PATH}:" \
        -e "s:\${subscribers}:${SUBSCRIBERS_FILE}:" ${SCALA_DIR}/${FILE} > ${SCALA_DIR}/${TMP_FILE}

    ${BASE_DIR}/runSpark.sh -v "${edmVersion}" -p "${propertiesFile}" -o "${sparkOptions}" -f "${SCALA_DIR}/${TMP_FILE}"
    if [ $? != 0 ]; then
        echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet for ${cur}"
        echo 1<&2 "ERROR: ${0}: Code executed: "
        cat "${SCALA_DIR}/${TMP_FILE}"
    fi
    testAndDeleteInvalidParquetFolder ${DEST_PATH}

    if [ $? != 0 ]; then
        echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet"
    fi
    # Cleaning
    rm ${SCALA_DIR}/${TMP_FILE}

    # Increment in a day
    cur=$((cur + 24*60*60))
done

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process save as parquet CS data, started at: ${startDate}, finished at: ${endDate}"

