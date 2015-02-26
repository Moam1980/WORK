#!/bin/bash

if [ -z "${dayToProcess}" ] || [ -z "${monthToProcess}" ] || [ -z "${yearToProcess}" ]; then
    echo 1>&2 "ERROR: ${0}: Year, month and day parameters are mandatories."
    exit 8 
fi

FILE="runModel.scala"
datePath="${yearToProcess}/${monthToProcess}/${dayToProcess}"
datePartialPath="${edmVersion%.[0-9]}/${datePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
sourcePath="${EVENTS_PARQUET_DIR}/${HADOOP_CS_PROBES_VERSION}/${datePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
dwell="${USER_CENTRIC_DIR}/dwell/${datePartialPath}"
journey="${USER_CENTRIC_DIR}/journey/${datePartialPath}"
jvp="${USER_CENTRIC_DIR}/jvp/${datePartialPath}"
#Get a timestamp to generate Scala file
TIMESTAMP=`date +%s`
TMP_FILE="runModel.tmp_${TIMESTAMP}.scala"
echo 1<&2 "INFO: ${0}: Testing if destination directory exists: "
echo 1>&2 "    dwell: ${dwell}"
echo 1>&2 "    journey: ${journey}"
echo 1>&2 "    jvp: ${jvp}"

testHdfsFolder ${sourcePath}
if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Cannot load events from ${sourcePath}"
fi
testAndDeleteInvalidParquetFolder ${dwell} ${journey} ${jvp}
if [ $? == 0 ]; then
    if [ ${overrideFlag} == "true" ]; then
        echo 1<&2 "INFO: Override flag is set to true. Deleting directores:"
        echo 1<&2 " ${dwell}"
        echo 1<&2 " ${journey}"
        echo 1<&2 " ${jvp}"
        hdfs dfs -rm -r ${dwell}
        hdfs dfs -rm -r ${journey}
        hdfs dfs -rm -r ${jvp}
    else
        echo 1<&2 "INFO: Skipping: ${dwell}" 
        echo 1<&2 "INFO: Skipping: ${journey}" 
        echo 1<&2 "INFO: Skipping: ${jvp}" 
        exit 0
    fi
fi

echo 1<&2 "INFO: ${0}: Preparing scala code from template" 
sed -e "s:\${source}:${sourcePath}:"\
    -e "s:\${dwell}:${dwell}:" \
    -e "s:\${journey}:${journey}:" \
    -e "s:\${jvp}:${jvp}:" \
    -e "s:\${CELL_CATALOGUE}:${CELL_CATALOGUE}:" \
    -e "s:\${subscribers}:${SUBSCRIBERS_FILE}:"\
    ${SCALA_DIR}/${FILE} > ${SCALA_DIR}/${TMP_FILE}

${BASE_DIR}/runSpark.sh -v "${edmVersion}" -p "${propertiesFile}" -o "${sparkOptions}" -f "${SCALA_DIR}/${TMP_FILE}"
if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet for ${cur}"
    echo 1<&2 "ERROR: ${0}: Code executed: "
    cat "${SCALA_DIR}/${TMP_FILE}"
fi
testAndDeleteInvalidParquetFolder ${dwell} ${journey} ${jvp}

if [ $? != 0 ]; then
    echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet"
fi
# Cleaning
rm ${SCALA_DIR}/${TMP_FILE}
