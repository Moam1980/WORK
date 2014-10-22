#!/bin/bash

# Get process PID
PROCESS_PID=$$

# Check base dir name
BASE_DIR=`dirname "$0"`

function usageHelp ()
{
    echo 1>&2 "Usage: $0 -s \<sql_filename\> -o \<output_filename\> [-c \<sql_condition\> -p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -s <sql_filename>: SQL file that contains query to export data to CSV, is mandatory"
    echo 1>&2 "    -o <output_filename>: Output filename, is mandatory"
    echo 1>&2 "    -c <sql_condition>: SQL condition to apply, is optional"
    echo 1>&2 "    -p <properties_file>: Properties file to use, is optional"
}

# Default values for optional parameters
PROPERTIES_FILE="${BASE_DIR}/properties/etl.properties"

# Check if number of parameters is the expected
if [ $# -ge 4 -a $# -le 8 ]; then

    # Check parameters
    while getopts s:o:c:p: o
    do    case "$o" in
        s)    SQL_FILENAME="$OPTARG";;
        o)    OUTPUT_FILE="${OPTARG}";;
        c)    SQL_CONDITION="${OPTARG}";;
        p)    PROPERTIES_FILE="${OPTARG}";;
        [?]) echo 1>&2 "ERROR:";usageHelp $*;exit 1;;
        esac
    done

else
    # Incorrect number of parameters
    echo 1>&2 "ERROR: $0: Number of parameters not correct: $#"
    usageHelp $*
    exit 1
fi

# Check PROPERTIES_FILE parameter
if [ -z ${PROPERTIES_FILE} ]; then
    echo 1>&2 "ERROR: $0: Properties file is mandatory"
    usageHelp $*
    exit 3
fi

# Check SQL_FILENAME
if [ -z ${SQL_FILENAME} ]; then
    echo 1>&2 "ERROR: $0: SQL query to export data to CSV is mandatory"
    usageHelp $*
    exit 4
fi

# Check OUTPUT_FILE
if [ -z ${OUTPUT_FILE} ]; then
    echo 1>&2 "ERROR: $0: Output filename to save data to is mandatory"
    usageHelp $*
    exit 5
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile ${PROPERTIES_FILE}


# Check that output file doesn't exists
checkNotExistsFile ${ISOP_OUTPUT_FILE_PATH}/${OUTPUT_FILE}

# Check that file to execute in database exists
FILE_TO_EXECUTE=${BASE_DIR}/sql/downloadToCsv.sql
checkIsReadableFile ${FILE_TO_EXECUTE}

# Check that SQL file exists
SQL_FILE=${BASE_DIR}/sql/${SQL_FILENAME}.sql
checkIsReadableFile ${SQL_FILE}

# Check LINE_SIZE
if [ -z ${LINE_SIZE} ]; then
    echo 1>&2 "ERROR: $0: Maximum length size to format properly CSV file is mandatory"
    usageHelp $*
    exit 6
fi

# Check DB_CONNECT_STRING parameter, expected to be defined in properties file
if [ -z ${DB_CONNECT_STRING} ]; then
    echo 1>&2 "ERROR: $0: DB_CONNECT_STRING is mandatory, check properties file"
    exit 7
fi

# Check DB_USER parameter, expected to be defined in properties file 
if [ -z ${DB_USER} ]; then
    echo 1>&2 "ERROR: $0: DB_USER is mandatory, check properties file"
    exit 9
fi

# Check DB_NAME parameter, expected to be defined in properties file 
if [ -z ${DB_PASSWORD} ]; then
    echo 1>&2 "ERROR: $0: DB_PASSWORD is mandatory, check properties file"
    exit 10
fi

CSV_FILE=${ISOP_OUTPUT_FILE_PATH}/${OUTPUT_FILE}.csv

# Get SQL query from file
SQL_QUERY=$(<${SQL_FILE})

# Get a timestamp to generate SQL file
TIMESTAMP=`date +%s`
TMP_SQL_FILE=${FILE_TO_EXECUTE}.tmp_${TIMESTAMP}.sql

# Get file to run and modify with parameters passed
sed "s|%%OUTPUT_FILE%%|${CSV_FILE}|g" ${FILE_TO_EXECUTE} > ${TMP_SQL_FILE}
sed -i "" "s|%%LINE_SIZE%%|${LINE_SIZE}|g" ${TMP_SQL_FILE}
sed -i "" "s/%%SQL_QUERY%%/${SQL_QUERY}/g" ${TMP_SQL_FILE}
sed -i "" "s/%%SQL_CONDITION%%/${SQL_CONDITION}/g" ${TMP_SQL_FILE}

# Download data from database
echo 1>&2 "INFO: $0: Downloading to file: ${CSV_FILE}"
sqlplus -S ${DB_USER}/${DB_PASSWORD}@${DB_CONNECT_STRING} @${TMP_SQL_FILE}

if [[ $? -eq 0 ]] ; then
    echo 1>&2 "INFO: $0: Deleting temporary files because it was downloaded correctly"
    rm -f ${TMP_SQL_FILE}
else
    echo 1>&2 "ERR: $0: There was an error downloading data to csv file: ${CSV_FILE}."
fi

exit 0
