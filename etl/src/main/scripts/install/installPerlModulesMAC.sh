#!/bin/bash

sudo cpan install DBI

brew --version
if [[ $? -eq 0 ]] ; then
    brew install unixodbc
    brew install freetds --with-unixodbc
else
    port version
    if [[ $? -eq 0 ]] ; then
        sudo port install unixodbc
        sudo port install freetds --with-unixodbc
    else
        echo 1>&2 "ERR: $0: You need Homebrew or MacPorts to continue"
        exit 1
    fi
fi

sudo cpan install DBD::ODBC
sudo cpan DateTime
sudo cpan install Geo::Coordinates::UTM
sudo cpan install DateTime::Format::Strptime
sudo cpan install String::CRC32
sudo cpan install Compress::Bzip2
sudo cpan install Geo::Coordinates
sudo cpan install Config::Properties

iodbctest "Driver=/usr/local/Cellar/freetds/0.91/lib/libtdsodbc.so;Server=localhost;Port=1433;TDS_Version=8.0;uid=USER;pwd=PASSWORD;Database=DATABASE"
