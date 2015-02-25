#!/bin/bash

if [ -z "${dayToProces}" && -z "${monthToProcess}" && -z "${yearToProcess}" ]; then
    echo 1>&2 "ERROR: ${0}: Year, month and day parameters are mandatories."
    exit 8 
fi

FILE="saveAsParquetFromXdr.scala"
count=0
while [ ${count} -lt ${#CS_DIRECTORIES[@]} ]
do
    csDirectory=${CS_DIRECTORIES[${count}]}
    dateSeparator=${CS_DATA_SEPARATOR[${count}]}

    # Define properties to run parse data process
    datePath="${yearToProcess}/${monthToProcess}/${dayToProcess}"
    basePath="${HADOOP_CS_PROBES_FILE_PATH}/${csDirectory}/${HADOOP_CS_PROBES_VERSION}/${datePath}"
    sourceDirectory="${basePath}/${HADOOP_CS_PROBES_FORMAT}"
    destinationDirectory="${basePath}/${HADOOP_CS_PROBES_PARQUET_FORMAT}"
    testHdfsFolder "${sourceDirectory}"
    if [ $? == 0 ]; then
        #Get a timestamp to generate SQL file
        TIMESTAMP=`date +%s`
        TMP_FILE="saveAsParquetFromXdr.tmp_${TIMESTAMP}.scala"
        echo 1<&2 "INFO: ${0}: Testing if destination directory exists: "
        echo 1>&2 "    destinationDirectory: ${destinationDirectory}"

        testAndDeleteInvalidParquetFolder ${destinationDirectory}
        if [ $? == 0 ]; then
            echo "The sourceDirectory already exists."
            if [ ${overrideFlag} == "true" ]; then
                echo "Override flag is set to true. Deleting directory: ${destinationDirectory}" 
                hdfs dfs -rm -r ${destinationDirectory}
            else
                echo "Skipping: ${destinationDirectory}" 
                exit 0
            fi
        fi

        # Get conversion method from source type
        if [[ ${sourceDirectory} = *A-Interface* ]]; then
            conversionMethod="toAiCsXdr"
        elif [[ ${sourceDirectory} = *IUCS-Interface* ]]; then
            conversionMethod="toIuCsXdr"
        fi

        echo 1<&2 "INFO: ${0}: Preparing scala code from template" 
        sed -e "s:\${origin}:${sourceDirectory}:" -e "s:\${destination}:${destinationDirectory}:" \
            -e "s:\${conversionMethod}:${conversionMethod}:" ${SCALA_DIR}/${FILE} > ${SCALA_DIR}/${TMP_FILE}

        ${BASE_DIR}/runSpark.sh -v "${edmVersion}" -p "${propertiesFile}" -o "${sparkOptions}"\
            -f "${SCALA_DIR}/${TMP_FILE}"
        if [ $? != 0 ]; then
            echo 1<&2 "ERROR: ${0}: Error occurred during save to parquet for ${cur}"
            echo 1<&2 "ERROR: ${0}: Code executed: "
            cat "${SCALA_DIR}/${TMP_FILE}"
        fi
        # Cleaning
        testAndDeleteInvalidParquetFolder ${destinationDirectory}
        rm ${SCALA_DIR}/${TMP_FILE}
    fi
    # Increment one the counter
    count=$((count + 1))
done
