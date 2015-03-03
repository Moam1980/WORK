#!/bin/bash

if [ -z "${dayToProcess}" ] || [ -z "${monthToProcess}" ] || [ -z "${yearToProcess}" ]; then
    echo 1>&2 "ERROR: ${0}: Year, month and day parameters are mandatories."
    exit 8 
fi

FILE="saveAsParquetFromEvent.scala"
sources=""
count=0
while [ ${count} -lt ${#CS_DIRECTORIES[@]} ]
do
    csDirectory=${CS_DIRECTORIES[${count}]}
    dateSeparator=${CS_DATA_SEPARATOR[${count}]}
    datePath="${yearToProcess}/${monthToProcess}/${dayToProcess}"
    basePath="${HADOOP_CS_PROBES_FILE_PATH}/${csDirectory}/${HADOOP_CS_PROBES_VERSION}/${datePath}"
    count=$((count + 1))
    if [ ! -z "${sources}" ]; then
        sources="${sources}, "
    fi
    sources="${sources}\"${basePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}\""
    # Increment one the counter
done
DEST_PATH="${EVENTS_PARQUET_DIR}/${HADOOP_CS_PROBES_VERSION}/${datePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
#Get a timestamp to generate temporal file
TIMESTAMP=`date +%s`
TMP_FILE=$(echo $FILE | sed -e "s/\./\.tmp_${TIMESTAMP}\./")
echo 1<&2 "INFO: ${0}: Testing if destination directory exists: "
echo 1>&2 "    DEST_PATH: ${DEST_PATH}"

testAndDeleteInvalidParquetFolder ${DEST_PATH}
if [ $? == 0 ]; then
    echo "The sourceDirectory already exists."
    if [ ${overrideFlag} == "true" ]; then
        echo "Override flag is set to true. Deleting directory: ${DEST_PATH}" 
        hdfs dfs -rm -r ${DEST_PATH}
    else
        echo "Skipping: ${DEST_PATH}" 
        exit 0
    fi
fi

echo 1<&2 "INFO: ${0}: Preparing scala code from template" 
sed -e "s:\${sources}:${sources}:" -e "s:\${destination_dir}:${DEST_PATH}:" \
    -e "s:\${subscribers}:${SUBSCRIBERS_FILE}:" ${SCALA_DIR}/${FILE} > ${SCALA_DIR}/${TMP_FILE}

${BASE_DIR}/runSpark.sh -v "${edmVersion}" -p "${propertiesFile}" -o "${sparkOptions}" -f "${SCALA_DIR}/${TMP_FILE}"
if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet for ${cur}"
    echo 1<&2 "ERROR: ${0}: Code executed: "
    cat "${SCALA_DIR}/${TMP_FILE}"
fi
testAndDeleteInvalidParquetFolder ${DEST_PATH}

if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet"
fi
# Cleaning
rm ${SCALA_DIR}/${TMP_FILE}
