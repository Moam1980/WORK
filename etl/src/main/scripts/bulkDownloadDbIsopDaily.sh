#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -d \<dateToDownload\> [-p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -d \<dateToDownload\>: date to get data from ISOP, is mandatory"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "     $0 -d 20141001"
    echo 1>&2 "             Is going to download information from Ipsos database for 1st of October 2014"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"

# Check if number of parameters is the expected
if [ $# -ge 2 -a $# -le 4 ]; then
    # Check parameters
    while getopts d:p: o
    do  case "${o}" in
        d)  dateToDownload="${OPTARG}";;
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

# Check date to download
if [ -z "${dateToDownload}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: date to download is mandatory: please include \"-d <dateToDownload>\" as a parameter"
    usageHelp $*
    exit 3
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
echo 1>&2 "    date to download: ${dateToDownload}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"

# Check that output path exists
checkIsDirectoryAndCreate ${OUTPUT_FILE_PATH}

# Check that isop outpath exists
checkIsDirectoryAndCreate ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}

# Check that log path exists
checkIsDirectoryAndCreate ${LOG_PATH}

# Get year, month and day from date
extractYearMonthDay ${dateToDownload}
yearToPush=$yearExtracted
monthToPush=$monthExtracted
dayToPush=$dayExtracted

# Download daily information for:
#   ia.T_IA_ALL_CNT
#   ia.T_IA_SUBS_SEARCHWORDS_D
#   ia.T_IA_APP_TYPE_CNT_VOL_D
#   ia.T_IA_APP_GROUP_CNT_VOL_D
#   ia.T_IA_SUBS_DOMAIN_D
#   ia.T_IA_SUBS_WEBPAGE_CAT_D
${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_ALL_CNT -o T_IA_ALL_CNT_${dateToDownload} -c "WHERE DATA_DAY = ${dateToDownload}" -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/aggregated-data/${HADOOP_ISOP_VERSION}" "T_IA_ALL_CNT_${dateToDownload}" "${HADOOP_ISOP_FORMAT}"

${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_SUBS_SEARCHWORDS_D -o T_IA_SUBS_SEARCHWORDS_D_${dateToDownload} -c "WHERE DATA_DAY = ${dateToDownload}" -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/subscribers-searches/${HADOOP_ISOP_VERSION}" "T_IA_SUBS_SEARCHWORDS_D_${dateToDownload}" "${HADOOP_ISOP_FORMAT}"

${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_APP_TYPE_CNT_VOL_D -o T_IA_APP_TYPE_CNT_VOL_D_${dateToDownload} -c "WHERE DATA_DAY = ${dateToDownload}" -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/subscribers-apps/${HADOOP_ISOP_VERSION}" "T_IA_APP_TYPE_CNT_VOL_D_${dateToDownload}" "${HADOOP_ISOP_FORMAT}"

${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_APP_GROUP_CNT_VOL_D -o T_IA_APP_GROUP_CNT_VOL_D_${dateToDownload} -c "WHERE DATA_DAY = ${dateToDownload}" -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/subscribers-apps-categories/${HADOOP_ISOP_VERSION}" "T_IA_APP_GROUP_CNT_VOL_D_${dateToDownload}" "${HADOOP_ISOP_FORMAT}"

${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_SUBS_DOMAIN_D -o T_IA_SUBS_DOMAIN_D_${dateToDownload} -c "WHERE DATA_DAY = ${dateToDownload}" -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/subscribers-domains/${HADOOP_ISOP_VERSION}" "T_IA_SUBS_DOMAIN_D_${dateToDownload}" "${HADOOP_ISOP_FORMAT}"

${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_SUBS_WEBPAGE_CAT_D -o T_IA_SUBS_WEBPAGE_CAT_D_${dateToDownload} -c "WHERE DATA_DAY = ${dateToDownload}" -p ${propertiesFile}
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/subscribers-domains-categories/${HADOOP_ISOP_VERSION}" "T_IA_SUBS_WEBPAGE_CAT_D_${dateToDownload}" "${HADOOP_ISOP_FORMAT}"

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process ISOP download for ${dateToDownload}, started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
