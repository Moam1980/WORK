#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -u \<user\> -l \<landing_server\> -s \<startDate\> -e \<endDate\> [-p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -u \<user\>: user in order to connect to remote server, is mandatory"
    echo 1>&2 "    -l \<landing_server\>: server to connect to where pulling to CS is being done, is mandatory"
    echo 1>&2 "    -s \<startDate\>: Start date to pull data, is mandatory"
    echo 1>&2 "    -e \<endDate\>: End date to pull data, is mandatory"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -u edm -l 10.64.246.168 -s 20141004 -e 20141205
      -- Download all CS files from 20141004 to 20141205 using 10.64.246.168 as landing server"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"
fileFormat=""

# Check if number of parameters is the expected
if [ $# -ge 8 -a $# -le 10 ]; then
    while getopts u:l:s:e:p: o
    do    case "${o}" in
        u)  user="${OPTARG}";;
        l)  server="${OPTARG}";;
        s)  startDate="${OPTARG}";;
        e)  endDate="${OPTARG}";;
        p)  propertiesFile="${OPTARG}";;
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
    echo 1<&2 "ERROR: ${0}: Server is mandatory: please include \"-s <server>\" as a parameter"
    usageHelp $*
    exit 4
fi

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    user: ${user}"
echo 1>&2 "    server: ${server}"
echo 1>&2 "    start date: ${startDate}"
echo 1>&2 "    end date: ${endDate}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"


# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Run pull data sftp cs catchup from landing server
ssh ${user}@${server} "cd;./etl-script-0.5.0/pullDataSftpCsCatchup.sh -s \"${startDate}\" -e \"${endDate}\" -p  \"${propertiesFile}\""

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process catching up with CS, started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
