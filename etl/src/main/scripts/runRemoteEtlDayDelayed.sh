#!/bin/bash

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -u \<user\> -l \<landing_server\> -c \<commnand\> -d \<delayDays\> [-p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -u \<user\>: user in order to connect to remote server, is mandatory"
    echo 1>&2 "    -l \<landing_server\>: server to connect to where command is going to be run, is mandatory"
    echo 1>&2 "    -c \<commnand\>: command to run in remote server, is mandatory"
    echo 1>&2 "    -d \<delayDays\>: Delay in days from today to run the command, is mandatory"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -u edm -l 10.64.246.168 -c "./${BASE_DIR}/pullDataSftpCsCatchup.sh" -d 1
      -- Download all CS files for yesterday as delay is 1 day using 10.64.246.168 as landing server"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"

# Check if number of parameters is the expected
if [ $# -ge 8 -a $# -le 10 ]; then
    while getopts u:l:c:d:p: o
    do    case "${o}" in
        u)  user="${OPTARG}";;
        l)  server="${OPTARG}";;
        c)  command="${OPTARG}";;
        d)  delayDays="${OPTARG}";;
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

# Check delayDays
if [ -z "${delayDays}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Delay in days from today is mandatory: please include \"-d <delayDays>\" as a parameter"
    usageHelp $*
    exit 3
fi

# Calculate date to run command, first get today date
todayDate=$(date +%s)
dateEpoc=$((todayDate - delayDays*24*60*60))

if [ "$(uname)" == "Darwin" ]; then
    # It is a mac
    dayCalculated=`date -jf "%s" $dateEpoc "+%Y%m%d"`
else
    dayCalculated=`date -d@$dateEpoc +%Y%m%d`
fi

# Execute remote command
${BASE_DIR}/runRemoteEtl.sh -u "${user}" -l "${server}" -c "${command}" -s "${dayCalculated}" -e "${dayCalculated}" -p "${propertiesFile}"
