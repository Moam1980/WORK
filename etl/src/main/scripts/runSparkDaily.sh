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
        -p <properties_file> -f <override_flag> -c <command_script>"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -s <startDate>: Start date for the command, is mandatory"
    echo 1>&2 "    -e <endDate>: End date for the command, is mandatory"
    echo 1>&2 "    -v <edmVersion>: Version of EDM core to use, is mandatory"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is mandatory"
    echo 1>&2 "    -f <override_flag>: Flag to override folder if exists, is mandatory"
    echo 1>&2 "    -o <spark_options>: Spark options to run process, is mandatory"
    echo 1>&2 "    -c <command_script>: Main script to run daily, is mandatory"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0}  -s 20141004 -e 20141205 -v 0.8.0 -o \"--master yarn-client --executor-memory 1g\"\
        -p \"properties/etl-config.properties\" -f \"false\" -c \"example.sh\"\
        -- Execute code of example.sh from 20141004 to 20141205 using 10.64.246.168 as master server"
}

# Check if number of parameters is the expected
if [ $# -eq 14 ]; then
    while getopts s:e:p:v:o:f:c: o
    do  case "${o}" in
        s)  startDate="${OPTARG}";;
        e)  endDate="${OPTARG}";;
        p)  propertiesFile="${OPTARG}";;
        v)  edmVersion="${OPTARG}";;
        o)  sparkOptions="${OPTARG}";;
        f)  overrideFlag="${OPTARG}";;
        c)  commandScript="${OPTARG}";;
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

# Check sparkOption parameter
if [ -z "${sparkOptions}" ]; then
    echo 1>&2 "ERROR: ${0}: Spark options are mandatory"
    usageHelp $*
    exit 8 
fi

# Check commandScript parameter
if [ -z "${commandScript}" ]; then
    echo 1>&2 "ERROR: ${0}: Command script are mandatory"
    usageHelp $*
    exit 9 
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh
SCALA_DIR="${BASE_DIR}/scala"

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
# convert in seconds since the epoch:
start=$(date -d"$startDate" +%s)
end=$(date -d"$endDate" +%s)
cur=$start

# Run for all days in period
while [ $cur -le $end ]; do
    # Get date
    extractYearMonthDayFromEpoc ${cur}
    yearToProcess=$yearExtracted
    monthToProcess=$monthExtracted
    dayToProcess=$dayExtracted

    source ${commandScript}

    # Increment in a day
    cur=$((cur + 24*60*60))
done

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process run spark daily of ${commandScript}, started at: ${startDate},\
    finished at: ${endDate}"
