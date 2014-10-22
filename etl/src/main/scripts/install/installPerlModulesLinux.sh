#!/bin/bash

sudo su -
yum install gcc freetds unixODBC unixODBC-devel
yum install perl perl-DBI
yum install perl-CPAN
cpan install DBI
cpan install DBD::ODBC
cpan install DateTime
cpan install Geo::Coordinates::UTM
cpan install DateTime::Format::Strptime
cpan install String::CRC32
cpan install Compress::Bzip2
cpan install Geo::Coordinates
cpan install DateTimeX::Auto
cpan install Config::Properties

echo "[FreeTDS] 
Driver = /usr/lib64/libtdsodbc.so.0" >> tds.driver.template

odbcinst -i -d -f tds.driver.template

odbcinst -q -d

echo "[MSSQL]
Driver = FreeTDS
Address = IP
Port = 1433
TDS Version = 8.0
Database = database" >> /etc/odbc.ini

isql -v -s MSSQL user password

