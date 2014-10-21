#!/bin/bash

# Function to get base name
getbasename () 
{
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: You need to specify file"
        exit 1
    fi

    # Get base name
    FILE_NAME=`basename $1 | cut -f1 -d '.'`

    # Check if it is empty
    if [ -z $FILE_NAME ]; then
        echo 1>&2 "ERROR: $0: Could not get base file name from: $1"
        exit 1
    fi
}

# Function to get extension of a file
getExtension () 
{
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: You need to specify file"
        exit 1
    fi

    # Get base name
    FILE_EXTENSION=`echo ${1##*.}`
    
    # Check if it is empty
    if [ -z $FILE_EXTENSION ]; then
        echo 1>&2 "ERROR: $0: Could not get extension for file name from: $1"
        exit 1
    fi
}

# Function to get a file name
getFileName () 
{
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: You need to specify file"
        exit 1
    fi

    # Get base name
    FILE_NAME=`basename $1`

    # Check if it is empty
    if [ -z $FILE_NAME ]; then
        echo 1>&2 "ERROR: $0: Could not get base file name from: $1"
        exit 1
    fi
}

# Function used to check if file exists and is a directory
# Return 0 if directory exist 1 if not exist and could be created and greater than 1
#if there is any problem
checkIsDirectory () 
{
    # Check that we have one parameter 
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: Directory name has to be specified"        # Exit entire script
        exit 1
    fi
    
    # Directory
    directory=$1
        
    # Check for deploy in order to do a new symbolic link
    if [ -e $directory ]; then
        # Check if not a directory
        if [ ! -d $directory ]; then
            echo 1>&2 "ERROR: $0: $directory is not a directory, please check it"
            # Exit entire script
            exit 2
        fi
    else
        echo 1>&2 "ERROR: $0: $directory does not exist, please check it"
        # Exit entire script
        exit 3    
    fi
}

# Function used to check if file exists and is a directory and create if doesn't exist
# Return 0 if directory exist or can be created and greater than 1
#if there is any problem
checkIsDirectoryAndCreate () 
{
    # Check that we have one parameter 
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: Directory name has to be specified"        # Exit entire script
        exit 1
    fi
    
    # Directory
    directory=$1
        
    # Check for deploy in order to do a new symbolic link
    if [ -e $directory ]; then
        # Check if not a directory
        if [ ! -d $directory ]; then
            echo 1>&2 "ERROR: $0: $directory is not a directory, please check it"
            # Exit entire script
            exit 2
        fi
    else
        # Directory does not exist just create
        mkdir -p ${directory}
        
        # Check if there is a problem creating directory
        if [ "$?" -ne "0" ]; then
              echo 1>&2 "ERROR: $0: $directory could not be created, please check it"
            # Exit entire script
            exit 3
        fi        
        
        # Directory has been created
        echo 1>&2 "INFO: $0: $directory has been created"
    fi
}

checkIsReadableFile () 
{
    # Check that we have one parameter 
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: File name has to be specified"
        # Exit entire script
        exit 1
    fi
    
    # Directory
    file=$1

    # Check that is a file and exist
    if [ ! -r ${file} ]; then 
        # Show error and exit
        echo 1<&2 "ERROR: $0: File: ${file} has to be a readable file"
        exit 2
    fi
}

checkNotExistsFile () 
{
    # Check that we have one parameter 
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: File name has to be specified"
        # Exit entire script
        exit 1
    fi
    
    # Directory
    file=$1

    # Check that is a file and exist
    if [ -e ${file} ]; then 
        # Show error and exit
        echo 1<&2 "ERROR: $0: File: ${file} exists, please make sure that file does not exists"
        exit 2
    fi
}

checkPropertiesFileParameter()
{
    # Check that we have one parameter 
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: Properties file name has to be specified"
        # Exit entire script
        exit 1
    fi
    
    # Checking directory
    checkIsReadableFile $1
}

loadPropertiesFile()
{
    checkPropertiesFileParameter $1
    
    # Load properties file
    . ${1}
    echo 1>&2 "INFO: $0: Properties file: ${1} has been loaded"
}



