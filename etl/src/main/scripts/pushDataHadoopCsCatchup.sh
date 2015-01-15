#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -s \<startDate\> -e \<endDate\> [-p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -s \<startDate\>: Start date to psuh data, is mandatory"
    echo 1>&2 "    -e \<endDate\>: End date to push data, is mandatory"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -s 20141004 -e 20141205
      -- Push all CS files from 20141004 to 20141205"
}

# Check if number of parameters is the expected
if [ $# -ge 4 -a $# -le 6 ]; then
    while getopts s:e:p: o
    do  case "${o}" in
        s)  startDate="${OPTARG}";;
        e)  endDate="${OPTARG}";;
        p)  propertiesFile="${OPTARG}";;
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
    exit 7
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile "${propertiesFile}"

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    start date: ${startDate}"
echo 1>&2 "    end date: ${endDate}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"

# This script should run from startDate to endDate
# convert in seconds sinch the epoch:
start=$(date -d"$startDate" +%s)
end=$(date -d"$endDate" +%s)
cur=$start

# Run for all days in period
while [ $cur -le $end ]; do
    # Get date
    yearToPush=`date -d@$cur +%Y`
    monthToPush=`date -d@$cur +%m`
    dayToPush=`date -d@$cur +%d`

    count=0
    while [ ${count} -lt ${#CS_DIRECTORIES[@]} ]
    do
        csDirectory=${CS_DIRECTORIES[${count}]}
        dateSeparator=${CS_DATA_SEPARATOR[${count}]}
        filePrefix=${CS_FILE_PREFIXES[${count}]}
        fileExtension=${CS_FILE_EXTENSIONS[${count}]}

        # Define properties to run pull data process
        localDirectory="${CS_LANDING_AREA}/${csDirectory}/${yearToPush}${monthToPush}${dayToPush}"
        formatFile="${filePrefix}${yearToPush}${dateSeparator}${monthToPush}${dateSeparator}${dayToPush}"
        formatFile="${formatFile}*${fileExtension}"

        # Create remote directories for year and month in Hadoop
        remoteDirectory="${HADOOP_CS_PROBES_FILE_PATH}/${csDirectory}/${HADOOP_CS_PROBES_VERSION}/${yearToPush}"
        hdfs dfs -mkdir -p ${remoteDirectory}
        # Check if everything went ok
        if [[ $? -ne 0 ]] ; then
            echo 1>&2 "ERROR: ${0}: Can't create remote directory in Hadoop: ${remoteDirectory}"
            exit 7
        fi

        # Create remote directory in Hadoop for month
        remoteDirectory="${remoteDirectory}/${monthToPush}"
        hdfs dfs -mkdir -p ${remoteDirectory}
        # Check if everything went ok
        if [[ $? -ne 0 ]] ; then
            echo 1>&2 "ERROR: ${0}: Can't create remote directory in Hadoop: ${remoteDirectory}"
            exit 7
        fi

        # Create remote directory in Hadoop for day
        remoteDirectory="${remoteDirectory}/${dayToPush}"
        hdfs dfs -mkdir -p ${remoteDirectory}
        # Check if everything went ok
        if [[ $? -ne 0 ]] ; then
            echo 1>&2 "ERROR: ${0}: Can't create remote directory in Hadoop: ${remoteDirectory}"
            exit 7
        fi

        # Run command to push data
        remoteDirectory="${remoteDirectory}/${HADOOP_CS_PROBES_FORMAT}"
        ${BASE_DIR}/pushDataHadoop.sh -r "${remoteDirectory}" -l "${localDirectory}" -f "${formatFile}" -p "${propertiesFile}"

        # Increment one the counter
        count=$((count + 1))
    done

    # Increment in a day
    cur=$((cur + 24*60*60))
done

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process catching up pushing CS data, started at: ${startDate}, finished at: ${endDate}"
