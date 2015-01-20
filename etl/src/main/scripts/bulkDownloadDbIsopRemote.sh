#!/bin/bash

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

# Execute remote command "cd;./etl-script-0.6.0/bulkDownloadDbIsopCatchup.sh"
${BASE_DIR}/runRemoteEtl.sh -c "cd;./etl-script-0.6.0/bulkDownloadDbIsopCatchup.sh" "$@"
