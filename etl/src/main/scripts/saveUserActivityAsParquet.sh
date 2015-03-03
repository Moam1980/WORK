#!/bin/bash

if [ -z "${dayToProcess}" ] || [ -z "${monthToProcess}" ] || [ -z "${yearToProcess}" ]; then
    echo 1>&2 "ERROR: ${0}: Year, month and day parameters are mandatories."
    exit 8 
fi

FILE="saveAsParquetFromUserActivity.scala"
datePath="${yearToProcess}/${monthToProcess}/${dayToProcess}"
datePartialPath="${edmVersion%.[0-9]}/${datePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
sourcePath="${EVENTS_PARQUET_DIR}/${HADOOP_CS_PROBES_VERSION}/${datePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
destination="${USER_ACTIVITY_DIR}/${datePartialPath}"
#Get a timestamp to generate Scala file
TIMESTAMP=`date +%s`
TMP_FILE=$(echo $FILE | sed -e "s/\./\.tmp_${TIMESTAMP}\./")
echo 1<&2 "INFO: ${0}: Testing if destination directory exists: "
echo 1>&2 "    destination: ${destination}"

testHdfsFolder ${sourcePath}
if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Cannot load events from ${sourcePath}"
    exit 1
fi
testAndDeleteInvalidParquetFolder ${destination} 
if [ $? == 0 ]; then
    if [ ${overrideFlag} == "true" ]; then
        echo 1<&2 "INFO: Override flag is set to true. Deleting directores:"
        echo 1<&2 " ${destination}"
        hdfs dfs -rm -r ${destination}
    else
        echo 1<&2 "INFO: Skipping: ${destination}" 
        exit 0
    fi
fi

echo 1<&2 "INFO: ${0}: Preparing scala code from template" 
sed -e "s:\${source}:${sourcePath}:"\
    -e "s:\${destination}:${destination}:" \
    -e "s:\${CELL_CATALOGUE}:${CELL_CATALOGUE}:" \
    ${SCALA_DIR}/${FILE} > ${SCALA_DIR}/${TMP_FILE}

${BASE_DIR}/runSpark.sh -v "${edmVersion}" -p "${propertiesFile}" -o "${sparkOptions}" -f "${SCALA_DIR}/${TMP_FILE}"
if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet for ${cur}"
    echo 1<&2 "ERROR: ${0}: Code executed: "
    cat "${SCALA_DIR}/${TMP_FILE}"
fi
testAndDeleteInvalidParquetFolder ${destination}

if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet"
    echo 1<&2 "ERROR:  Code executed:" 
    cat ${SCALA_DIR}/${TMP_FILE} 
fi
# Cleaning
rm ${SCALA_DIR}/${TMP_FILE}
