#!/bin/bash

sudo su -
cd /tmp
echo "Downloading latest parallel..."
wget http://ftp.gnu.org/gnu/parallel/parallel-latest.tar.bz2

echo "Unpacking..."
tar xvjf parallel-latest.tar.bz2
rm -f parallel-latest.tar.bz2

echo "Building..."
cd parallel-*
./configure
make && sudo make install
ln -s /usr/local/bin/parallel /usr/bin/parallel
