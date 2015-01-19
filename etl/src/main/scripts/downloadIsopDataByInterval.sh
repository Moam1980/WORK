#!/bin/sh
#/bin/bash

# Get process PID
PROCESS_PID=$$

# Check base dir name
BASE_DIR=`dirname "$0"`

function usageHelp ()
{
    echo 1>&2 "Usage: $0 -s \<start_day\> -e \<end_day\> [-p \<properties_file\> ]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "     -s <start_day>: Initial date, is mandatory"
    echo 1>&2 "     -e <end_day>: End date, is mandatory"
    echo 1>&2 "     -p <properties_file>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "     $0 -s 20141001 -e 20141020"
    echo 1>&2 "             Is going to download information from Ipsos database, starting at 1st till 20th of October"
}

# Default values for optional parameters
PROPERTIES_FILE="${BASE_DIR}/properties/config-etl.sh"

# Check if number of parameters is the expected
if [ $# -ge 4 -a $# -le 6 ]; then
    # Check parameters
        while getopts s:e:p: o
        do      case "$o" in
                s)      DSTART="$OPTARG";;
                e)      DEND="$OPTARG";;
                p)      PROPERTIES_FILE="${OPTARG}";;
                [?]) echo 1>&2 "ERROR:";usageHelp;exit 1;;
                esac
        done
else
        # Incorrect number of parameters (1 or 2 pairs expected)
        echo 1>&2 "ERROR: $0: Number of parameters not correct: $#"
        usageHelp $*
        exit 1
fi

# Check DSTART
if [ -z ${DSTART} ]; then
    echo 1>&2 "ERROR: $0: Start date to analyse is mandatory"
    usageHelp $*
    exit 3
fi

# Check DEND
if [ -z ${DEND} ]; then
    echo 1>&2 "ERROR: $0: End date to analyse is mandatory"
    usageHelp $*
    exit 4
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile ${PROPERTIES_FILE}

# Check that output path exists
checkIsDirectoryAndCreate ${OUTPUT_FILE_PATH}

# Check that isop outpath exists
checkIsDirectoryAndCreate ${BULK_DOWNLOAD_OUTPUT_FILE_PATH}

# Check that log path exists
checkIsDirectoryAndCreate ${LOG_PATH}

# Create file with dates to download
DATES_FILE=${BULK_DOWNLOAD_OUTPUT_FILE_PATH}/dates_to_download_${DSTART}_${DEND}.tmp
checkNotExistsFile ${DATES_FILE}

if [ "$(uname)" == "Darwin" ]; then
    # It is a mac
    # convert in seconds sinch the epoch:
    start=$(date -jf "%Y%m%d" $DSTART "+%s")
    end=$(date -jf "%Y%m%d" $DEND "+%s")
    
    cur=$start
    echo 
    # Create file with dates
    while [ $cur -le $end ]; do
        # convert seconds to date:
        DAY=`date -jf "%s" $cur "+%Y%m%d"`
    
        echo "$DAY" >> ${DATES_FILE}
        cur=$((cur + 24*60*60))
    done
    
else
    # convert in seconds sinch the epoch:
    start=$(date -d$DSTART +%s)
    end=$(date -d$DEND +%s)
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

# Download information from all tables including condition in SQL
# First download information not related with date
${BASE_DIR}/bulkDownloadDB.sh -s "download-T_IA_CFG_CAT_TREE" -o "T_IA_CFG_CAT_TREE_${DSTART}_${DEND}"

# Download daily information using parallels for:
#   ia.T_IA_ALL_CNT
#   ia.T_IA_SUBS_SEARCHWORDS_D
#   ia.T_IA_APP_TYPE_CNT_VOL_D
#   ia.T_IA_APP_GROUP_CNT_VOL_D
#   ia.T_IA_SUBS_DOMAIN_D
#   ia.T_IA_SUBS_WEBPAGE_CAT_D
cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_T_IA_ALL_CNT_${DSTART}_${DEND}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_ALL_CNT -o T_IA_ALL_CNT_{} -c \"WHERE DATA_DAY = {}\" -p ${PROPERTIES_FILE}"
cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_T_IA_SUBS_SEARCHWORDS_D_${DSTART}_${DEND}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_SUBS_SEARCHWORDS_D -o T_IA_SUBS_SEARCHWORDS_D_{} -c \"WHERE DATA_DAY = {}\" -p ${PROPERTIES_FILE}"
cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_T_IA_APP_TYPE_CNT_VOL_D_${DSTART}_${DEND}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_APP_TYPE_CNT_VOL_D -o T_IA_APP_TYPE_CNT_VOL_D_{} -c \"WHERE DATA_DAY = {}\" -p ${PROPERTIES_FILE}"
cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_T_IA_APP_GROUP_CNT_VOL_D_${DSTART}_${DEND}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_APP_GROUP_CNT_VOL_D -o T_IA_APP_GROUP_CNT_VOL_D_{} -c \"WHERE DATA_DAY = {}\" -p ${PROPERTIES_FILE}"
cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_T_IA_SUBS_DOMAIN_D_${DSTART}_${DEND}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_SUBS_DOMAIN_D -o T_IA_SUBS_DOMAIN_D_{} -c \"WHERE DATA_DAY = {}\" -p ${PROPERTIES_FILE}"
cat ${DATES_FILE}  | parallel --joblog ${DOWNLOAD_LOG_FILE}_T_IA_SUBS_WEBPAGE_CAT_D_${DSTART}_${DEND}.log --no-notice --progress -k -v -P ${ISOP_DOWNLOAD_PARALLEL_PROCS} -n 1 -I{} "${BASE_DIR}/bulkDownloadDB.sh -s download-T_IA_SUBS_WEBPAGE_CAT_D -o T_IA_SUBS_WEBPAGE_CAT_D_{} -c \"WHERE DATA_DAY = {}\" -p ${PROPERTIES_FILE}"

# Removing dates file
echo 1>&2 "Removing dates file: $DATES_FILE"
rm ${DATES_FILE}
exit 0
