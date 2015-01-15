#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -r \<remote_directory\> -l \<local_directory\>
        -f \<format_file\> [-p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -r \<remote_directory\>: remote directory where files have to be pushed, is mandatory"
    echo 1>&2 "    -l \<local_directory\>: local directory where files can be found, is mandatory"
    echo 1>&2 "    -f \<format_file\>: format of the file names to push, is mandatory"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -r hdfs://10.64.247.224/user/tdatuser/welcome-sms/0.3/2014/10/12/csv/
      -l /data/landing/welcome-sms/1.0/20141012 -f \"*_TTS_2.LOG\"
      -- Push welcome sms to Hadoop for 20141012"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"

# Check if number of parameters is the expected
if [ $# -ge 6 -a $# -le 8 ]; then
    while getopts r:l:f:p: o
    do    case "${o}" in
        r)  remoteDirectory="${OPTARG}";;
        l)  localDirectory="${OPTARG}";;
        f)  fileFormat="${OPTARG}";;
        p)  propertiesFile="${OPTARG}";;
        [?]) echo 1>&2 "ERROR: ${0}:";usageHelp $*;exit 1;;
        esac
    done
else
    # Incorrect number of parameters
    echo 1>&2 "ERROR: ${0}: Number of parameters not correct: $#"
    usageHelp $*
    exit 2
fi

if [ -z "${remoteDirectory}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Remote directory is mandatory: please include \"-r \<remote_directory\>\" as a parameter"
    usageHelp $*
    exit 3
fi

if [ -z "${localDirectory}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Local directory is mandatory: please include \"-l \<local_directory\>\" as a parameter"
    usageHelp $*
    exit 4
fi

if [ -z "${fileFormat}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: File format is mandatory: please include \"-f \<format_file\>\" as a parameter"
    usageHelp $*
    exit 5
fi

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    remoteDirectory: ${remoteDirectory}"
echo 1>&2 "    localDirectory: ${localDirectory}"
echo 1>&2 "    fileFormat: ${fileFormat}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"

# Check propertiesFile parameter
if [ -z "${propertiesFile}" ]; then
    echo 1>&2 "ERROR: ${0}: Properties file is mandatory"
    usageHelp $*
    exit 6
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile "${propertiesFile}"

# Check if local directory exists, if not we do not need to continue, there is nothing to upload
checkIsDirectory "${localDirectory}"

# Create remote directory in Hadoop
hdfs dfs -mkdir -p ${remoteDirectory}
# Check if everything went ok
if [[ $? -ne 0 ]] ; then
    echo 1>&2 "ERROR: ${0}: Can't create remote directory in Hadoop: ${remoteDirectory}"
    exit 7
fi

# Put files to Hadoop
hdfs dfs -put ${localDirectory}/${fileFormat} ${remoteDirectory}
# Check if everything went ok
if [[ $? -ne 0 ]] ; then
    echo 1>&2 "ERROR: ${0}: There was an error pushing data to Hadoop, checking files transferred"
fi

# Now we have to check files to remove ones copied correctly, there are two alternatives using MD5 or size
# Right now we are going to use Size of the files
timestamp=`date +%s`
localFilesWithSize="${localDirectory}/localFilesSize_${timestamp}.tmp"
remoteFilesWithSize="${localDirectory}/remoteFilesSize_${timestamp}.tmp"
# List local files
ls -al ${localDirectory}/${fileFormat} | sed -e's/  */ /g' | cut -d " " -f5,8 > ${localFilesWithSize}

# List files in Hadoop
hdfs dfs -ls ${remoteDirectory}/${fileFormat} | sed -e's/  */ /g' | cut -d " " -f5,8 > ${remoteFilesWithSize}

# Check that both files have been generated correctly
checkIsReadableFile ${localFilesWithSize}
checkIsReadableFile ${remoteFilesWithSize}

# Load in memory remote files with size, to make easier comparison, as this one should be the bigger
count=0
while read line
do
    # Get file name and size
    remoteFilesSize[${count}]=`echo "${line}" | cut -d " " -f1`
    fileName=`echo "${line}" | sed -e"s/${remoteFilesSize[${count}]} //g"`
    remoteFilesBaseName[${count}]=`basename "${fileName}"`
    count=`expr ${count} + 1`
done < "${remoteFilesWithSize}"

# Get number of files already downloaded
numberOfFilesRemotely=${count}

# Number of files to push
numberOfFilesLocally=`wc -l "${localFilesWithSize}" | sed -e's/^ *//g' | cut -d " " -f1`

# Check local files against remote ones
numberOfDifferentFiles=0
count_found=0
while read line
do
    # Get content from file
    fileSize=`echo "${line}" | cut -d " " -f1`
    fileName=`echo "${line}" | sed -e"s/${fileSize} //g"`
    fileBaseName=`basename "${fileName}"`

    # Check if the file has been already pushed correctly
    count_pushed=0
    found=0
    foundDifferent=0
    while [ ${count_pushed} -lt ${#remoteFilesBaseName[@]} ]
    do
        if [ "${remoteFilesBaseName[${count_pushed}]}" == "${fileBaseName}"  ]; then
            # Check size
            if [ "${remoteFilesSize[${count_pushed}]}" == "${fileSize}" ]; then
                found=1
                break
            else
                # File was found but different size
                foundDifferent=1
            fi
        fi
        count_pushed=`expr ${count_pushed} + 1`
    done

    # Check if the file was pushed correctly we should delete file
    if [ ${found} -eq 1 ]; then
        echo 1>&2 "INFO: ${0}: File ${localDirectory}/${fileBaseName} pushed correctly, deleting."
        echo rm ${localDirectory}/${fileBaseName}
        count_found=`expr ${count} + 1`
    else
        echo 1>&2 "ERR: ${0}: File ${localDirectory}/${fileBaseName} not pushed correctly."
    fi

    # Check if the file was changed
    if [ ${foundDifferent} -eq 1 ]; then
        echo 1>&2 "INFO: ${0}: File ${localDirectory}/${fileBaseName} found with different size, local: ${fileSize}."
        numberOfDifferentFiles=`expr ${numberOfDifferentFiles} + 1`
    fi
done < "${localFilesWithSize}"

# Remove temp files
rm "${localFilesWithSize}"
rm "${remoteFilesWithSize}"

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary file of execution
summaryFileExecutions="${localDirectory}/summaryPushExecution.csv"

# Check if exists
if [ ! -r "${summaryFileExecutions}" ]; then
    # File does not exists create with header
    header="Start Time UTC|End Time UTC|Remote Directory|Local Directory|File Format|Properties file"
    header="${header}|Number of files to push|Number of files pushed correctly"
    header="${header}|Number of different files (different size)"
    echo "${header}" > "${summaryFileExecutions}"
fi

# Write summary of execution
line="${startTimestampUtc}|${endTimestampUtc}|${remoteDirectory}|${localDirectory}|${fileFormat}|${propertiesFile}"
line="${line}|${numberOfFilesLocally}|${count_found}|${numberOfDifferentFiles}"

echo "${line}" >> "${summaryFileExecutions}"
