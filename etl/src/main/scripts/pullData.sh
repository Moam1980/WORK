#!/bin/bash

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "$0"`

function usageHelp ()
{
	echo 1>&2 "Usage: $0 -u \<user\> -s \<server\> -r \<remote_directory\> -l \<local_directory\>
	    [-p \<properties_file\>]"
	echo 1>&2 "Parameters:"
	echo 1>&2 "    -u \<user\>: user in order to connect to remote server, is mandatory"
	echo 1>&2 "    -s \<server\>: server to connect to, is mandatory"
	echo 1>&2 "    -r \<remote_directory\>: remote directory where files to download are, is mandatory"
	echo 1>&2 "    -l \<local_directory\>: local directory to download the data, is mandatory"
	echo 1>&2 "    -p \<properties_file\>: Properties file to use, is optional"
	echo 1>&2 "Examples:"
	echo 1>&2 "	$0 -u edm -s 10.64.246.168 -r /data/welcome_sms/intf/20141001 -l /data/landing/welcome_sms/1.0/20141001
	  -- Download welcome sms to landing area"
}

# Default values for optional parameters
propertiesFile="${BASE_DIR}/properties/config-etl.sh"

# Check if number of parameters is the expected
if [ $# -ge 8 -a $# -le 10 ]; then
	while getopts u:s:r:l:p: o
	do	case "$o" in
		u)	user="$OPTARG";;
		s)	server="$OPTARG";;
		r)	remoteDirectory="$OPTARG";;
		l)	localDirectory="$OPTARG";;
		p)  propertiesFile="${OPTARG}";;
		[?]) echo 1>&2 "ERROR: $0:";usageHelp $*;exit 1;;
		esac
	done
else
    # Incorrect number of parameters
    echo 1>&2 "ERROR: $0: Number of parameters not correct: $#"
    usageHelp $*
    exit 2
fi

# Check user, server, remote and local directories
if [ -z ${user} ]; then 
	# Show error and exit
	echo 1<&2 "ERROR: $0: User is mandatory: please include \"-u <user>\" as a parameter"
	usageHelp $*
	exit 3
fi

if [ -z ${server} ]; then 
	# Show error and exit
	echo 1<&2 "ERROR: $0: Server is mandatory: please include \"-s <server>\" as a parameter"
	usageHelp $*
	exit 4
fi

if [ -z ${remoteDirectory} ]; then 
	# Show error and exit
	echo 1<&2 "ERROR: $0: Remote directory is mandatory: please include \"-r \<remote_directory\>\" as a parameter"
	usageHelp $*
	exit 5
fi

if [ -z ${localDirectory} ]; then
	# Show error and exit
	echo 1<&2 "ERROR: $0: Local directory is mandatory: please include \"-l \<local_directory\>\" as a parameter"
	usageHelp $*
	exit 6
fi

echo 1<&2 "INFO: $0: Running with following parameters: "
echo 1>&2 "    user: ${user}"
echo 1>&2 "    server: ${server}"
echo 1>&2 "    remoteDirectory: ${remoteDirectory}"
echo 1>&2 "    localDirectory: ${localDirectory}"
echo 1>&2 "    propertiesFile: ${propertiesFile}"

# Check propertiesFile parameter
if [ -z ${propertiesFile} ]; then
    echo 1>&2 "ERROR: $0: Properties file is mandatory"
    usageHelp $*
    exit 7
fi

# Initialize parameters and functions
. ${BASE_DIR}/functions.sh

# Load properties file
loadPropertiesFile ${propertiesFile}

# Check if local directory exists
checkIsDirectoryAndCreate "${localDirectory}"

# Check if we have a local file with files already download
filesWithLocalMd5="${localDirectory}/localFilesMd5.success"

# Check that is a file and exist
if [ -r "${filesWithLocalMd5}" ]; then
    # File doesn't exists we have to download all remote files
    count=0
    while read line
    do
        # Get file name and md5
        localFilesMd5[${count}]=`echo "$line" | cut -d " " -f1`
        fileName=`echo "$line" | sed -e"s/${localFilesMd5[${count}]} //g"`
        localFilesBaseName[${count}]=`basename "${fileName}"`
        #echo 1>&2 "INFO: $0: File already downloaded ${localFilesBaseName[${count}]} with: ${localFilesMd5[${count}]}"
        count=`expr ${count} + 1`
    done < "${filesWithLocalMd5}"
fi

# Get a timestamp to create temporally file
timestamp=`date +%s`
filesWithRemoteMd5="${localDirectory}/remoteFilesMd5_${timestamp}.tmp"

# Check remote directory files and calculate MD5
ssh ${user}@${server} "find \"${remoteDirectory}\" -type f -maxdepth 1 -exec md5sum {} \\;"  > "$filesWithRemoteMd5"

# Check if there were any error
if [[ $? -ne 0 ]] ; then
    echo 1>&2 "ERROR: $0: There was a problem checking remote directory."
    exit 8
fi

count=0
while read line
do
    # Get content from file
    fileMd5=`echo "$line" | cut -d " " -f1`
    fileName=`echo "$line" | sed -e"s/${fileMd5} //g"`
    fileBaseName=`basename "${fileName}"`

    # Check if the file has been already downloaded
    count_downloaded=0
    found=0
    while [ ${count_downloaded} -lt ${#localFilesBaseName[@]} ]
    do
        if [ "${localFilesBaseName[${count_downloaded}]}" == "$fileBaseName"  ]; then
            # Check MD5
            if [ "${localFilesMd5[${count_downloaded}]}" == "$fileMd5" ]; then
                found=1
                break
            fi
        fi
        count_downloaded=`expr ${count_downloaded} + 1`
    done

    # Check if the file was downlaoded correctly previously
    if [ $found -eq 0 ]; then
        # Add file name and md5
        filesMd5[${count}]=$fileMd5
        filesBaseName[${count}]=$fileBaseName
        #echo 1>&2 "INFO: $0: File not found ${filesBaseName[${count}]} with: ${filesMd5[${count}]}"
        count=`expr ${count} + 1`
    fi
done < "${filesWithRemoteMd5}"

# Just download new ones
total_files=`wc -l "$filesWithRemoteMd5" | sed -e's/  */ /g' | cut -d " " -f2`

# Remove temp files
rm "$filesWithRemoteMd5"

# Check if all files has been downloaded
if [ ${#filesBaseName[@]} -eq 0 ]; then
    echo 1>&2 "INFO $0: no files to download, of a total of $total_files"
    exit 0
fi

echo 1>&2 "INFO $0: Starting download of ${#filesBaseName[@]} files, of a total of $total_files"
count=0
while [ ${count} -lt ${#filesBaseName[@]} ]
do
    # Download file
    scp  ${user}@${server}:"\"${remoteDirectory}/${filesBaseName[${count}]}\"" "${localDirectory}"

    if [[ $? -eq 0 ]] ; then
        # Calculate md5 for downloaded file in order to check that download was correct
        if [ "$(uname)" == "Darwin" ]; then
            # It is a mac, check md5
            localMd5=`md5 "${localDirectory}/${filesBaseName[${count}]}" | cut -d "=" -f2 | sed -e's/  *//g'`
        else
            # It is a Linux
            localMd5=`md5sum "${localDirectory}/${filesBaseName[${count}]}" | cut -d " " -f1`
        fi

        # Check MD5
        if [ "${localMd5}" == "${filesMd5[${count}]}" ]; then
            echo 1>&2 "INFO: $0: File: ${filesBaseName[${count}]} downloaded correctly."
            # Add downloaded file to sucess file
            echo "${filesMd5[${count}]} ${filesBaseName[${count}]}" >> "${filesWithLocalMd5}"
        else
            # there was an error downloading file
            echo 1>&2 "ERROR $0: Downloading: ${filesBaseName[${count}]} remote Md5: ${filesMd5[${count}]}
                not match local Md5: ${localMd5}"
            rm "${localDirectory}/${filesBaseName[${count}]}"
        fi
    fi
    count=`expr ${count} + 1`
done

echo 1>&2 "INFO $0: ${#filesBaseName[@]} files downloaded"