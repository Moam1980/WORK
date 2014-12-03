#!/usr/bin/perl -w
#
# TODO: License goes here!
#
#---------------------------------------------------------------------
# Description of program
#
#   Check files in a directory per period of 10 minutes and size
#
#---------------------------------------------------------------------

use strict;
use warnings;

use Config::Properties;
use Cwd;
use DateTime;
use DateTime::Format::Strptime qw();
use File::Basename;
use Getopt::Long;
use POSIX qw(strftime);

# Get base directory and default properties file location
my ${baseDir}            = cwd($0);
my ${propertiesFileName} = "${baseDir}/properties/checkFilesSftp.properties";
my ${startDate}          = '2014-10-04';
my ${endDate}            = strftime "%Y-%m-%d", localtime(time-86400);

GetOptions(
    'propertiesFileName=s' => \${propertiesFileName},
    'startDate=s'          => \${startDate},
    'endDate=s'            => \${endDate}
);

# Open properties file for reading
open( my ${propertiesFile}, "<${propertiesFileName}" )
  or die "ERROR: $0: Can't open ${propertiesFileName} for reading: +$!\n";

# Load properties file
my ${properties} = Config::Properties->new();
${properties}->load( ${propertiesFile} );

# Get configuration from properties
my ${directory} = ${properties}->getProperty("directory")
  or die "ERROR: $0: Can't get property: directory from: ${propertiesFileName}: +$!\n";
my ${fileName} = ${properties}->getProperty("fileName")
  or die "ERROR: $0: Can't get property: fileName from: ${propertiesFileName}: +$!\n";
my ${filePattern} = ${properties}->getProperty("filePattern")
  or die "ERROR: $0: Can't get property: filePattern from: ${propertiesFileName}: +$!\n";
my ${outputPath} = ${properties}->getProperty("outputPath")
  or die "ERROR: $0: Can't get property: outputPath from: ${propertiesFileName}: +$!\n";
my ${filesIntervalMinutes} = ${properties}->getProperty("filesIntervalMinutes")
  or die "ERROR: $0: Can't get property: filesIntervalMinutes from: ${propertiesFileName}: +$!\n";
my ${sourceName} = ${properties}->getProperty("sourceName")
  or die "ERROR: $0: Can't get property: sourceName from: ${propertiesFileName}: +$!\n";

# Close properties file
close( ${propertiesFile} );

print "INFO: Parameters are:\n";
print " Directory to check: ${directory} \n";
print " Filename to check: ${fileName} \n";
print " File pattern is: ${filePattern} \n";
print " Start date to check: ${startDate} \n";
print " End date to check: ${endDate} \n";
print " Output path: ${outputPath} \n";
print " Interval between files: ${filesIntervalMinutes} \n";
print " Source name: ${sourceName} \n";

my ( ${year}, ${month}, ${day} ) = split '\-', ${startDate};
my ${dtStart} = DateTime->new(
    year   => ${year},
    month  => ${month},
    day    => ${day},
    hour   => 0,
    minute => 0,
    second => 0,
);
( ${year}, ${month}, ${day} ) = split '\-', ${endDate};
my ${dtEnd} = DateTime->new(
    year   => ${year},
    month  => ${month},
    day    => ${day},
    hour   => 0,
    minute => 0,
    second => 0,
);

my %files_size;
my %files_count;

# Initialize arrays
while ( ${dtStart} < ${dtEnd} ) {

    # File size as zero as well as count of number of files
    $files_size{ ${dtStart} }  = 0;
    $files_count{ ${dtStart} } = 0;

    # Add interval time
    ${dtStart}->add( minutes => ${filesIntervalMinutes} );
}

my @files = <${directory}${fileName}>;

# Check all files
foreach my ${file} (@files) {
    print "Analysing file: ${file}\n";

    open( my $inputFile, "<${file}" )
      or die "ERROR: $0: Can't open $file for reading: +$!\n";

    while (<$inputFile>) {
        my ( ${line} ) = $_;
        chomp( ${line} );

        if ( ${line} =~ m|$filePattern| ) {

            # Get data
            my (
                ${fileSize},     ${fileStartYear}, ${fileStartMonth},
                ${fileStartDay}, ${fileStartHour}, ${fileStartMin},
                ${fileStartSec}, ${fileEndYear},   ${fileEndMonth},
                ${fileEndDay},   ${fileEndHour},   ${fileEndMin},
                ${fileEndSec}
            ) = ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13 );

            ${dtStart} = DateTime->new(
                year   => ${fileStartYear},
                month  => ${fileStartMonth},
                day    => ${fileStartDay},
                hour   => ${fileStartHour},
                minute => ${fileStartMin},
                second => ${fileStartSec},
            );

            ${dtEnd} = DateTime->new(
                year   => ${fileEndYear},
                month  => ${fileEndMonth},
                day    => ${fileEndDay},
                hour   => ${fileEndHour},
                minute => ${fileEndMin},
                second => ${fileEndSec},
            );

            # Check if interval is correct in file name
            if ( ${dtEnd} !=
                ( ${dtStart}->add( minutes => ${filesIntervalMinutes} ) ) )
            {
                print "ERROR: $0: Wrong interval of this file from: ",
                  ${fileStartYear}, ${fileStartMonth}, ${fileStartDay},
                  ${fileStartHour}, ${fileStartMin},
                  ${fileStartSec}, "to: ", ${fileEndYear}, ${fileEndMonth},
                  ${fileEndDay}, ${fileEndHour},
                  ${fileEndMin}, ${fileEndSec}, " \n";
            }
            else {
                # Generate arrays with size and number of downloads
                $files_size{ ${dtStart} } = ${fileSize};
                $files_count{ ${dtStart} } += 1;
            }
        }
        else {
            print "ERROR: $0: Can't match line: ${line}\n";
        }
    }

    close( ${inputFile} );
}

# Store results in a file including zeros
my ${outputFileName} = "${outputPath}/checkFiles_${sourceName}_${startDate}_${endDate}.csv";
open( my $outputFile, ">${outputFileName}" )
  or die "ERROR: $0: Can't open ${outputFileName} for writing : +$!\n";
print $outputFile "Source Name|Date|Time|DateTime|File Size Bytes|Number of files\n";

my ${key};
foreach ${key}( sort keys %files_size ) {

    # Print each line including size and number of files
    if ( ${key} =~ m|^(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)$| ) {
        my ( ${year}, ${month}, ${day}, ${hour}, ${minutes}, ${seconds} ) =
          ( $1, $2, $3, $4, $5, $6 );
        my ${dateFormatted} = "${year}${month}${day}|${hour}:${minutes}:${seconds}";
        print $outputFile "${sourceName}|${dateFormatted}|${key}|${files_size{${key}}}|${files_count{${key}}}\n";
    }
}

print "Generated output in: ${outputFileName}\n";
exit 0
