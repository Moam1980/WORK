#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -u \<user\> -l \<landing_server\>"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -u \<user\>: user in order to connect to remote server, is mandatory"
    echo 1>&2 "    -l \<landing_server\>: server to connect to where command is going to be run, is mandatory"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -u edm -l 10.64.246.168
      -- It is going to connect to tdatuser@10.64.247.242 generate a list of all files and run process to
      generate a CSV file for DQ Hadoop"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"

# Check if number of parameters is the expected
if [ $# -eq 4 ]; then
    while getopts u:l: o
    do    case "${o}" in
        u)  user="${OPTARG}";;
        l)  server="${OPTARG}";;
        [?]) echo 1>&2 "ERROR: ${0}:";usageHelp $*;exit 1;;
        esac
    done
else
    # Incorrect number of parameters
    echo 1>&2 "ERROR: ${0}: Number of parameters not correct: $#"
    usageHelp $*
    exit 2
fi

# Check user, server
if [ -z "${user}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: User is mandatory: please include \"-u <user>\" as a parameter"
    usageHelp $*
    exit 3
fi

if [ -z "${server}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Server is mandatory: please include \"-l <landing_server>\" as a parameter"
    usageHelp $*
    exit 4
fi

# Calculate date to run command, first get today date
dateEpoc=$(date +%s)

if [ "$(uname)" == "Darwin" ]; then
    # It is a mac
    dayCalculated=`date -jf "%s" $dateEpoc "+%Y%m%d"`
else
    dayCalculated=`date -d@$dateEpoc +%Y%m%d`
fi

# Execute list in hadoop server
ssh ${user}@${server} "hdfs dfs -ls -R" | grep -v ".Trash" | sed -e's/  */ /g' | cut -d " " -f5,8 | grep -v "^0 " > /data/landing/hdfs-files.csv

# Run script to generate CSV for Tableau
${BASE_DIR}/checkFilesHadoop.pl

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
