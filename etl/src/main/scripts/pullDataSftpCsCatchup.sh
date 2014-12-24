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
    echo 1>&2 "    -s \<startDate\>: Start date to pull data, is mandatory"
    echo 1>&2 "    -e \<endDate\>: End date to pull data, is mandatory"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -u edm -s 20141004 -e 20141205
      -- Download all CS files from 20141004 to 20141205"
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
    yearToDownload=`date -d@$cur +%Y`
    monthToDownload=`date -d@$cur +%m`
    dayToDownload=`date -d@$cur +%d`

    count=0
    while [ ${count} -lt ${#CS_DIRECTORIES[@]} ]
    do
        csDirectory=${CS_DIRECTORIES[${count}]}
        dateSeparator=${CS_DATA_SEPARATOR[${count}]}
        filePrefix=${CS_FILE_PREFIXES[${count}]}
        fileExtension=${CS_FILE_EXTENSIONS[${count}]}

        # Define properties to run pull data process
        localDirectory="${CS_LANDING_AREA}/${csDirectory}/${yearToDownload}${monthToDownload}${dayToDownload}"
        formatFile="${filePrefix}${yearToDownload}${dateSeparator}${monthToDownload}${dateSeparator}${dayToDownload}"
        formatFile="${formatFile}*${fileExtension}"

        # Run command in to pull data via sftp, using unprocessed directory
        remoteDirectory="${csDirectory}/unprocessed"
        ${BASE_DIR}/pullDataSftp.sh -u \"${CS_USER}\" -s \"${CS_SERVER}\" -r  \"${remoteDirectory}\" -l \"${localDirectory}\" -f \"${formatFile}\"

        # Run command again but using date to get files
        remoteDirectory="${csDirectory}/${yearToDownload}${monthToDownload}${dayToDownload}"
        # Run command in landing server to pull data via sftp
        ${BASE_DIR}/pullDataSftp.sh -u \"${CS_USER}\" -s \"${CS_SERVER}\" -r  \"${remoteDirectory}\" -l \"${localDirectory}\" -f \"${formatFile}\"

        # We should check previous and following days in case there is a problem with data
        # Get yesterday date
        yesterday=$((cur - 24*60*60))
        yearToDownload=`date -d@$yesterday +%Y`
        monthToDownload=`date -d@$yesterday +%m`
        dayToDownload=`date -d@$yesterday +%d`
        remoteDirectory="${csDirectory}/${yearToDownload}${monthToDownload}${dayToDownload}"
        # Run command in landing server to pull data via sftp
        ${BASE_DIR}/pullDataSftp.sh -u \"${CS_USER}\" -s \"${CS_SERVER}\" -r  \"${remoteDirectory}\" -l \"${localDirectory}\" -f \"${formatFile}\"

        # Get following day
        followingDay=$((cur + 24*60*60))
        yearToDownload=`date -d@$followingDay +%Y`
        monthToDownload=`date -d@$followingDay +%m`
        dayToDownload=`date -d@$followingDay +%d`
        remoteDirectory="${csDirectory}/${yearToDownload}${monthToDownload}${dayToDownload}"
        # Run command in landing server to pull data via sftp
        ${BASE_DIR}/pullDataSftp.sh -u \"${CS_USER}\" -s \"${CS_SERVER}\" -r  \"${remoteDirectory}\" -l \"${localDirectory}\" -f \"${formatFile}\"

        # Increment one the counter
        count=$((count + 1))
    done

    # Increment in a day
    cur=$((cur + 24*60*60))
done

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process catching up with CS, started at: ${startDate}, finished at: ${endDate}"
