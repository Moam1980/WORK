#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} [-h -p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -h: If information download should de pushed to Hadoop, is optional"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "     $0"
    echo 1>&2 "             Is going to download information from IMS database using default configuration"
    echo 1>&2 "     $0 -h"
    echo 1>&2 "             Is going to download information from IMS database using default configuration and push information to Hadoop"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl-IMS.sh"
pushHadoopFlag=0

# Check if number of parameters is the expected
if [ $# -ge 0 -a $# -le 2 ]; then
    # Check parameters
    while getopts p:h o
    do  case "${o}" in
        p)  propertiesFile="${OPTARG}";;
        h)  pushHadoopFlag=1;;
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
    exit 7
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile "${propertiesFile}"

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    propertiesFile: ${propertiesFile}"
if [ ${pushHadoopFlag} == 1 ]; then
    echo 1>&2 "    push to Hadoop: true"
else
    echo 1>&2 "    push to Hadoop: false"
fi

# Check that output path exists
checkIsDirectoryAndCreate ${OUTPUT_FILE_PATH}

# Check that isop outpath exists
checkIsDirectoryAndCreate ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}

# Check that log path exists
checkIsDirectoryAndCreate ${LOG_PATH}

# Get today date
today=$(date +%Y%m%d)
extractYearMonthDay ${today}
yearToPush=$yearExtracted
monthToPush=$monthExtracted
dayToPush=$dayExtracted

# Download daily information for:
#   IMS_MOBILY.cell_view1
#   IMS_MOBILY.cell_view2
${BASE_DIR}/bulkDownloadDB.sh -s download-IMS_MOBILY.CELL_VIEW1 -o cell_view1_${today} -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_IMS_FILE_PATH}/cell-view1/${HADOOP_IMS_VERSION}" "cell_view1_${today}" "${HADOOP_IMS_FORMAT}" ${pushHadoopFlag}

${BASE_DIR}/bulkDownloadDB.sh -s download-IMS_MOBILY.CELL_VIEW2 -o cell_view2_${today} -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_IMS_FILE_PATH}/cell-view2/${HADOOP_IMS_VERSION}" "cell_view2_${today}" "${HADOOP_IMS_FORMAT}" ${pushHadoopFlag}

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process IMS download for ${today}, started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
