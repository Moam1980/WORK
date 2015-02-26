#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -u <user> -l <master_server> -c <commnand> -s <startDate> -e <endDate> \
        -v <edm_version> -o <spark_options> [-p <properties_file>] [-f <override_flag>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -u <user>: user in order to connect to remote server, is mandatory"
    echo 1>&2 "    -l <master_server>: server to connect to where command is going to be run, is mandatory"
    echo 1>&2 "    -c <commnand>: command to run in remote server, is mandatory"
    echo 1>&2 "    -s <startDate>: Start date for the command, is mandatory"
    echo 1>&2 "    -e <endDate>: End date for the command, is mandatory"
    echo 1>&2 "    -v <edmVersion>: Version of EDM core to use, is mandatory"
    echo 1>&2 "    -o <spark_options>: Spark options to run process, is mandatory"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is optional"
    echo 1>&2 "    -f <override_flag>: Flag to override folder if exists, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -u edm -l 10.64.246.168 -c \"${BASE_DIR}/saveEventAsParquet.sh\" -s 20141004 \
        -e 20141205 -v 0.8.0 -o \"--master yarn-client --executor-memory 1g\"
      -- Convert and save to Parquet data files from 20141004 to 20141205 using 10.64.246.168 as master server"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"
overrideFlag="false"

# Check if number of parameters is the expected
if [ $# -ge 14 -a $# -le 18 ]; then
    while getopts u:l:c:s:e:v:p:o:f: o
    do    case "${o}" in
        u)  user="${OPTARG}";;
        l)  server="${OPTARG}";;
        c)  command="${OPTARG}";;
        s)  startDate="${OPTARG}";;
        e)  endDate="${OPTARG}";;
        v)  edmVersion="${OPTARG}";;
        p)  propertiesFile="${OPTARG}";;
        o)  sparkOptions="${OPTARG}";;
        f)  overrideFlag="${OPTARG}";;
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
    echo 1<&2 "ERROR: ${0}: Server is mandatory: please include \"-l <master_server>\" as a parameter"
    usageHelp $*
    exit 4
fi
if [ -z "${command}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Command is mandatory: please include \"-c <command>\" as a parameter"
    usageHelp $*
    exit 5
fi

if [ -z "${edmVersion}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: EDM version is mandatory: please include \"-v <edm_version>\" as a parameter"
    usageHelp $*
    exit 6
fi

if [ -z "${sparkOptions}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Spark options are  mandatory: please include \"-o <spark_options>\" as a parameter"
    usageHelp $*
    exit 7
fi

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    user: ${user}"
echo 1>&2 "    server: ${server}"
echo 1>&2 "    command: ${command}"
echo 1>&2 "    startDate: ${startDate}"
echo 1>&2 "    endDate: ${endDate}"
echo 1>&2 "    edmVersion: ${edmVersion}"
echo 1>&2 "    sparkOptions: ${sparkOptions}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"
echo 1>&2 "    overrideFlag: ${overrideFlag}"

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Run remote script from master server
ssh ${user}@${server} ". ~/.bash_profile; ./etl-script-${edmVersion}/runSparkDaily.sh -c \"${command}\"\
    -s \"${startDate}\" -e \"${endDate}\" -p  \"${propertiesFile}\" -v \"${edmVersion}\" -o \"${sparkOptions}\"\
    -f \"${overrideFlag}\"" 

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
