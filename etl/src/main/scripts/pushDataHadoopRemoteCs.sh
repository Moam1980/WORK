#!/bin/bash

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

# Execute remote command "cd;${BASE_DIR}/pushDataHadoopCsCatchup.sh"
${BASE_DIR}/runRemoteEtl.sh -c "cd;${BASE_DIR}/pushDataHadoopCsCatchup.sh" "$@"
