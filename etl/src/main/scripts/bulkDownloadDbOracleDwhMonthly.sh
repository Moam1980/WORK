#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -m <month_download> [-h -p <properties_file>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -m: Month to be downloaded, is mandatory"
    echo 1>&2 "    -h: If information downloaded should de pushed to Hadoop, is optional"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "     $0 -m 201501"
    echo 1>&2 "             Is going to download subscribers at January 2015 from Oracle DWH database using \
        default configuration"
    echo 1>&2 "     $0 -m 201501 -h"
    echo 1>&2 "             Is going to download subscribers at January 2015 from Oracle DWH database using \
        default configuration and push information to Hadoop"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl-Oracle-DWH.sh"
pushHadoopFlag=0

# Check if number of parameters is the expected
if [ $# -ge 2 -a $# -le 5 ]; then
    # Check parameters
    while getopts m:p:h o
    do  case "${o}" in
        m)  monthToDownload="${OPTARG}";;
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

# Check monthToDownload parameter
if [ -z "${monthToDownload}" ]; then
    echo 1>&2 "ERROR: ${0}: Month to download is mandatory"
    usageHelp $*
    exit 3
fi

# Check propertiesFile parameter
if [ -z "${propertiesFile}" ]; then
    echo 1>&2 "ERROR: ${0}: Properties file is mandatory"
    usageHelp $*
    exit 4
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile "${propertiesFile}"

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    monthToDownload: ${monthToDownload}"
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

# Get this month date
extractYearMonthDay "${monthToDownload}01"
yearToPush=$yearExtracted
monthToPush=$monthExtracted
dayToPush=$dayExtracted

# Download information for subscribers
${BASE_DIR}/bulkDownloadDB.sh -s download-Oracle-DWH.SUBSCRIBERS -o subscribers_${monthToDownload}01 \
    -p ${propertiesFile} \
    -c ", F_CSTM_IN_DUMPS_HISTORY PARTITION(part_${monthToDownload}01)IN_DUMPS, \
        F_CSTM_HLR_STS PARTITION(part_${monthToDownload}01) HLR, \
        MV_MONTHLY_MU_GEO_MRKT PARTITION(MV_MU_GEO_MRKT_${monthToDownload}) MU \
        WHERE A.MSISDN=IN_DUMPS.MSISDN(+) AND A.MSISDN=HLR.MSISDN(+) AND \
            A.MSISDN=IVR.MSISDN AND \
            A.MSISDN=CRM.MSISDN(+) AND \
            A.MSISDN=MU.MSISDN(+) AND \
            IVR.STATUS(+)=1 AND \
            crm.active_status<>3"
pushDataHadoop $? $yearToPush $monthToPush $dayToPush \
    "${HADOOP_ORACLE_DWH_FILE_PATH}/subscribers/${HADOOP_ORACLE_DWH_VERSION}" "subscribers_${monthToDownload}01" \
    "${HADOOP_ORACLE_DWH_FORMAT}" ${pushHadoopFlag}

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process Oracle DWH download for ${monthToDownload}01, started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
