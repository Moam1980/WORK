#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function usageHelp ()
{
    echo 1>&2 "Usage: ${0} -u \<user\> -s \<server\> -r \<remote_directory\> -l \<local_directory\>
        [-f \<format_file\> -p \<properties_file\>]"
    echo 1>&2 "Parameters:"
    echo 1>&2 "    -u \<user\>: user in order to connect to remote server, is mandatory"
    echo 1>&2 "    -s \<server\>: server to connect to, is mandatory"
    echo 1>&2 "    -r \<remote_directory\>: remote directory where files to download are, is mandatory"
    echo 1>&2 "    -l \<local_directory\>: local directory to download the data, is mandatory"
    echo 1>&2 "    -f \<format_file\>: format of the file names to download, is optional"
    echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
    echo 1>&2 "Examples:"
    echo 1>&2 "    ${0} -u edm -s 10.64.246.168 -r /data/welcome_sms/intf/20141001 -l /data/landing/welcome_sms/1.0/20141001
      -- Download welcome sms to landing area"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"
fileFormat=""

# Check if number of parameters is the expected
if [ $# -ge 8 -a $# -le 12 ]; then
    while getopts u:s:r:l:f:p: o
    do    case "${o}" in
        u)    user="${OPTARG}";;
        s)    server="${OPTARG}";;
        r)    remoteDirectory="${OPTARG}";;
        l)    localDirectory="${OPTARG}";;
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

# Check user, server, remote and local directories
if [ -z "${user}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: User is mandatory: please include \"-u <user>\" as a parameter"
    usageHelp $*
    exit 3
fi

if [ -z "${server}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Server is mandatory: please include \"-s <server>\" as a parameter"
    usageHelp $*
    exit 4
fi

if [ -z "${remoteDirectory}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Remote directory is mandatory: please include \"-r \<remote_directory\>\" as a parameter"
    usageHelp $*
    exit 5
fi

if [ -z "${localDirectory}" ]; then
    # Show error and exit
    echo 1<&2 "ERROR: ${0}: Local directory is mandatory: please include \"-l \<local_directory\>\" as a parameter"
    usageHelp $*
    exit 6
fi

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    user: ${user}"
echo 1>&2 "    server: ${server}"
echo 1>&2 "    remoteDirectory: ${remoteDirectory}"
echo 1>&2 "    localDirectory: ${localDirectory}"
echo 1>&2 "    fileFormat: ${fileFormat}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"

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

# Check if local directory exists
checkIsDirectoryAndCreate "${localDirectory}"

# Check if we have a local file with files already download
filesWithLocalSize="${localDirectory}/localFilesSize.success"

# Check that is a file and exist
count=0
if [ -r "${filesWithLocalSize}" ]; then
    # File doesn't exists we have to download all remote files
    while read line
    do
        # Get file name and size
        localFilesSize[${count}]=`echo "${line}" | cut -d " " -f1`
        fileName=`echo "${line}" | sed -e"s/${localFilesSize[${count}]} //g"`
        localFilesBaseName[${count}]=`basename "${fileName}"`
        count=`expr ${count} + 1`
    done < "${filesWithLocalSize}"
fi

# Get number of files already downloaded
numberOfFilesLocally=${count}

# Get a timestamp to create temporally file
timestamp=`date +%s`
filesWithRemoteSize="${localDirectory}/remoteFilesSize_${timestamp}.tmp"

# Check remote directory files and calculate size
lftp -u "${user}," -e "cls -1 -s --block-size=1 \"${remoteDirectory}/${fileFormat}\";quit" sftp://${server} \
    | sed -e's/^ *//g'  > "${filesWithRemoteSize}"

# Check if there were any error
if [[ $? -ne 0 ]] ; then
    echo 1>&2 "ERROR: ${0}: There was a problem checking remote directory."
    exit 8
fi

# Get number of remote files
numberOfFilesRemotely=`wc -l "${filesWithRemoteSize}" | sed -e's/^ *//g' | cut -d " " -f1`

numberOfDifferentFiles=0
count=0
while read line
do
    # Get content from file
    fileSize=`echo "${line}" | cut -d " " -f1`
    fileName=`echo "${line}" | sed -e"s/${fileSize} //g"`
    fileBaseName=`basename "${fileName}"`

    # Check if the file has been already downloaded
    count_downloaded=0
    found=0
    foundDifferent=0
    while [ ${count_downloaded} -lt ${#localFilesBaseName[@]} ]
    do
        if [ "${localFilesBaseName[${count_downloaded}]}" == "${fileBaseName}"  ]; then
            # Check size
            if [ "${localFilesSize[${count_downloaded}]}" == "${fileSize}" ]; then
                found=1
                break
            else
                # File was found but different size
                foundDifferent=1
            fi
        fi
        count_downloaded=`expr ${count_downloaded} + 1`
    done

    # Check if the file was downlaoded correctly previously
    if [ ${found} -eq 0 ]; then
        # Add file name and size
        filesSize[${count}]=${fileSize}
        filesBaseName[${count}]=${fileBaseName}
        #echo 1>&2 "INFO: ${0}: File not found ${filesBaseName[${count}]} with: ${filesSize[${count}]}"
        count=`expr ${count} + 1`
    fi

    # Check if the file was changed
    if [ ${foundDifferent} -eq 1 ]; then
        # Delete previous version of the file and add it to counter
        rm "${localDirectory}/${fileBaseName}"
        numberOfDifferentFiles=`expr ${numberOfDifferentFiles} + 1`
    fi
done < "${filesWithRemoteSize}"

# Remove temp files
rm "${filesWithRemoteSize}"

filesDownloaded=0
# Check if all files has been downloaded
if [ ${#filesBaseName[@]} -eq 0 ]; then
    echo 1>&2 "INFO ${0}: no files to download, of a total of ${numberOfFilesRemotely}"
else
    echo 1>&2 "INFO ${0}: Starting download of ${#filesBaseName[@]} files, of a total of ${numberOfFilesRemotely}"
    count=0
    while [ ${count} -lt ${#filesBaseName[@]} ]
    do
        # Download file, overriding if neccesary
        command="get -O \"${localDirectory}\" \"${remoteDirectory}/${filesBaseName[${count}]}\""
        lftp -u "${user}," -e "set xfer:clobber on;${command};quit" sftp://${server}

        if [[ $? -eq 0 ]] ; then
            # Calculate size for downloaded file in order to check that download was correct
            if [ "$(uname)" == "Darwin" ]; then
                # It is a mac, check size
                localSize=`stat -f %z "${localDirectory}/${filesBaseName[${count}]}"`
            else
                # It is a Linux
                localSize=`stat -c %s "${localDirectory}/${filesBaseName[${count}]}"`
            fi

            # Check size
            if [ "${localSize}" == "${filesSize[${count}]}" ]; then
                echo 1>&2 "INFO: ${0}: File: ${filesBaseName[${count}]} downloaded correctly."
                # Add downloaded file to sucess file
                echo "${filesSize[${count}]} ${filesBaseName[${count}]}" >> "${filesWithLocalSize}"
                filesDownloaded=`expr ${filesDownloaded} + 1`
            else
                # there was an error downloading file
                echo 1>&2 "ERROR ${0}: Downloading: ${filesBaseName[${count}]} remote size: ${filesSize[${count}]}
                    not match local size: ${localSize}"
                rm "${localDirectory}/${filesBaseName[${count}]}"
            fi
        fi
        count=`expr ${count} + 1`
    done

    echo 1>&2 "INFO ${0}: ${filesDownloaded} files downloaded"
fi

# End time stamp
endTimestampUtc=`date -u  "+%Y%m%d %H:%M:%S"`

# Write summary file of execution
summaryFileExecutions="${localDirectory}/summaryFileExecution.csv"

# Check if exists
if [ ! -r "${summaryFileExecutions}" ]; then
    # File does not exists create with header
    header="Start Time UTC|End Time UTC|Remote User|Remote Server|Remote Directory|Local Directory|File Format"
    header="${header}|Properties file|Number of files downloaded already|Number of files remote directory"
    header="${header}|Number of different files (different size)|Number of files to download|Number of files download"
    echo "${header}" > "${summaryFileExecutions}"
fi

# Write summary of execution
line="${startTimestampUtc}|${endTimestampUtc}|${user}|${server}|${remoteDirectory}|${localDirectory}|${fileFormat}"
line="${line}|${propertiesFile}|${numberOfFilesLocally}|${numberOfFilesRemotely}"
line="${line}|${numberOfDifferentFiles}|${#filesBaseName[@]}|${filesDownloaded}"

echo "${line}" >> "${summaryFileExecutions}"
