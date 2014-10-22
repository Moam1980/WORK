#!/bin/bash

brew --version
if [[ $? -eq 0 ]] ; then
    brew install parallel
else
    port version
    if [[ $? -eq 0 ]] ; then
        sudo port install parallel
    else
        echo 1>&2 "ERR: $0: You need Homebrew or MacPorts to continue"
        exit 1
    fi
fi

