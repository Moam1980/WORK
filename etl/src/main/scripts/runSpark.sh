#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -v <edm_version> -o <spark_options> -p <properties_file> -f <scala_file>"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -v <edm_version>: EDM core version, is mandatory"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is mandatory"
    echo 1>&2 "    -o <spark_options>: Spark options to use, is mandatory"
    echo 1>&2 "    -f <scala_file>: File with scala code to run" 
    echo 1>&2 "    ${0} -v \"0.7.0\" -o \"--master yarn-client --executor-memory 1g\"\
        -p \"properties/etl-config.properties\" -f \"/tmp/etl/saveEvents.scala\""
}

# Check if number of parameters is the expected
if [ $# -eq 8 ]; then
    while getopts p:v:o:f: o
    do case "${o}" in
        p)  propertiesFile="${OPTARG}";;
        v)  edmVersion="${OPTARG}";;
        o)  sparkOptions="${OPTARG}";;
        f)  scalaFile="${OPTARG}";;
        [?]) echo 1>&2 "ERROR: ${0}:";usageHelp $*;exit 1;;
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

if [ -z "${edmVersion}" ]; then
    echo 1<&2 "ERROR: ${0}: EDM version is mandatory: please include \"-v <edm_version>\" as a parameter"
    usageHelp $*
    exit 6
fi

if [ -z "${sparkOptions}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Spark options are mandatory: please include \"-o <spark_options>\" as a parameter"
    usageHelp $*
    exit 7
fi

# Check overrideFlag parameter
if [ -z "${scalaFile}" ]; then
    echo 1>&2 "ERROR: ${0}: Scala file is mandatory: please include \"-f <scala_file> as a parameter\""
    usageHelp $*
    exit 8
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh
loadPropertiesFile "${propertiesFile}"

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    edmVersion: ${edmVersion}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"
echo 1>&2 "    sparkOptions: ${sparkOptions}"
echo 1>&2 "    scalaFile: ${scalaFile}"
echo 1>&2 "    sparkDir: ${SPARK_ARTIFACTS_DIR}" 

spark-shell ${sparkOptions} --jars ${SPARK_ARTIFACTS_DIR}/edm-core-complete-assembly-${edmVersion}.jar < ${scalaFile}

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished Spark process, started at: ${startDate}, finished at: ${endDate}"

