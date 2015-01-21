#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -s \<startDate\> -e \<endDate\> [-h -p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -s \<startDate\>: Start date to get data from ISOP, is mandatory"
    echo 1>&2 "    -e \<endDate\>: End date to get data, is mandatory"
    echo 1>&2 "    -h: If information download should de pushed to Hadoop, is optional"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "     $0 -s 20141001 -e 20141020"
    echo 1>&2 "             Is going to download information from Ipsos database, starting at 1st till 20th of October"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"
pushHadoopFlag=0

# Check if number of parameters is the expected
if [ $# -ge 4 -a $# -le 7 ]; then
    # Check parameters
    while getopts s:e:p:h o
    do  case "${o}" in
        s)  startDate="${OPTARG}";;
        e)  endDate="${OPTARG}";;
        h)  pushHadoopFlag=1;;
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

# Check endDate
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
if [ ${pushHadoopFlag} == 1 ]; then
    echo 1>&2 "    push to Hadoop: true"
    pushHadoopOption="-h"
else
    echo 1>&2 "    push to Hadoop: false"
    pushHadoopOption=""
fi

# Check that output path exists
checkIsDirectoryAndCreate ${OUTPUT_FILE_PATH}

# Check that isop outpath exists
checkIsDirectoryAndCreate ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}

# Check that log path exists
checkIsDirectoryAndCreate ${LOG_PATH}

# Create file with dates to download
DATES_FILE=${BULK_DOWNLOAD_OUTPUT_FILE_PATH}/dates_to_download_${startDate}_${endDate}.tmp
checkNotExistsFile ${DATES_FILE}

# Get today date
today=$(date +%Y%m%d)
extractYearMonthDay ${today}
yearToPush=$yearExtracted
monthToPush=$monthExtracted
dayToPush=$dayExtracted

# First download information not related with date, we should add this information for the day the process is executed
${BASE_DIR}/bulkDownloadDB.sh -s "download-T_IA_CFG_CAT_TREE" -o "T_IA_CFG_CAT_TREE_${today}"
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/categories/${HADOOP_ISOP_VERSION}" "T_IA_CFG_CAT_TREE_${today}" "${HADOOP_ISOP_FORMAT}" ${pushHadoopFlag}

# Download latest version of subscriber, we need first last date of the table been updated
${BASE_DIR}/bulkDownloadDB.sh -s "download-LAST_DATE_OFR_SUBS_HIS_D" -o "SUBSCRIBERS_LAST_DATE_${startDate}_${endDate}.tmp"

# Check if file exists
checkIsReadableFile ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}/SUBSCRIBERS_LAST_DATE_${startDate}_${endDate}.tmp.csv

# Load subscriber last date
. ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}/SUBSCRIBERS_LAST_DATE_${startDate}_${endDate}.tmp.csv
extractYearMonthDay ${SUBSCRIBERS_LAST_DATE}
yearToPush=$yearExtracted
monthToPush=$monthExtracted
dayToPush=$dayExtracted

# Download subscribers and push data
${BASE_DIR}/bulkDownloadDB.sh -s "download-OFR_SUBS_HIS_D" -o "subscribers_${SUBSCRIBERS_LAST_DATE}"
pushDataHadoop $? $yearToPush $monthToPush $dayToPush "${HADOOP_ISOP_FILE_PATH}/subscribers/${HADOOP_ISOP_VERSION}" "subscribers_${SUBSCRIBERS_LAST_DATE}" "${HADOOP_ISOP_FORMAT}" ${pushHadoopFlag}

# Remove last day for subscribers file
rm ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}/SUBSCRIBERS_LAST_DATE_${startDate}_${endDate}.tmp.csv

# Download information from all tables including condition in SQL
# Check if it is only one day or more
if [ "${startDate}" == "${endDate}" ]; then
    # It is same day we don't need parallel to run download
    ${BASE_DIR}/bulkDownloadDbIsopDaily.sh -d "${startDate}" -p "${propertiesFile}" ${pushHadoopOption}
else
    # Check operating system to generate dates for parallel
    if [ "$(uname)" == "Darwin" ]; then
        # It is a mac
        # convert in seconds sinch the epoch:
        start=$(date -jf "%Y%m%d" $startDate "+%s")
        end=$(date -jf "%Y%m%d" $endDate "+%s")

        cur=$start

        # Create file with dates
        while [ $cur -le $end ]; do
            # convert seconds to date:
            DAY=`date -jf "%s" $cur "+%Y%m%d"`

            echo "$DAY" >> ${DATES_FILE}
            cur=$((cur + 24*60*60))
        done
    else
        # convert in seconds sinch the epoch:
        start=$(date -d$startDate +%s)
        end=$(date -d$endDate +%s)
        cur=$start
        # Create file with dates
        while [ $cur -le $end ]; do
            # convert seconds to date:
            DAY=`date -d@$cur +%Y-%m-%d | tr -d -`

            echo "$DAY" >> ${DATES_FILE}
            cur=$((cur + 24*60*60))
        done
    fi

    # Check file exists with dates
    checkIsReadableFile ${DATES_FILE}

    # Download daily information using parallels
    cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_bulkDownloadDbIsopCatchup_${startDate}_${endDate}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDbIsopDaily.sh -d \"{}\" -p \"${propertiesFile}\"  ${pushHadoopOption} "

    # Removing dates file
    echo 1>&2 "Removing dates file: $DATES_FILE"
    rm ${DATES_FILE}
fi

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process catching up with ISOP, started at: ${startTimestampUtc}, finished at: ${endTimestampUtc}"
