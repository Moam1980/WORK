#!/usr/bin/perl -w
#
# TODO: License goes here!
#
#---------------------------------------------------------------------
# Description of program
#
#   Check files in Hadoop grouping by source and date including
#       - total size
#       - total number of files
#
#---------------------------------------------------------------------

use strict;
use warnings;

use Config::Properties;
use Cwd;
use DateTime::Format::Strptime qw();
use Getopt::Long;
use POSIX qw(strftime);

# Get base directory and default properties file location
my ${baseDir}            = cwd($0);
my ${propertiesFileName} = "${baseDir}/properties/checkFilesHadoop.properties";
my ${todayDate}            = strftime "%Y-%m-%d", localtime( time );

GetOptions(
    'propertiesFileName=s' => \${propertiesFileName},
    'todayDate=s'          => \${todayDate}
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
my ${outputPath} = ${properties}->getProperty("outputPath")
  or die "ERROR: $0: Can't get property: outputPath from: ${propertiesFileName}: +$!\n";

# Close properties file
close( ${propertiesFile} );

print "INFO: Parameters are:\n";
print " Directory to check: ${directory} \n";
print " Filename to check: ${fileName} \n";
print " Output path: ${outputPath} \n";
print " Today date: ${todayDate} \n";

my $format = DateTime::Format::Strptime->new(
    pattern   => '%Y-%m-%d',
    time_zone => 'local',
    on_error  => 'croak',
);

# Define matrix for size and files with source
my %lastDatePerSource;

# Store results in a file including zeros
my ${outputFileName} =
  "${outputPath}/checkFilesHadoop_${todayDate}.csv";
open( my $outputFile, ">${outputFileName}" )
  or die "ERROR: $0: Can't open ${outputFileName} for writing : +$!\n";
print $outputFile "Source Name|Format|Date|File Name|Size Bytes\n";

# Check all files
my @files = <${directory}${fileName}>;
foreach my ${file}(@files) {
    print "Analysing file: ${file}\n";

    open( my $inputFile, "<${file}" )
      or die "ERROR: $0: Can't open $file for reading: +$!\n";

    while (<$inputFile>) {
        my ( ${line} ) = $_;
        chomp( ${line} );

        # Define variables
        my (
            ${fileSize}, ${sourceName}, ${fileYear}, ${fileMonth},
            ${fileDay},  ${fileFormat}, ${fileName}
        );
        if ( ${line} =~ m|^(\d+) (.*?)/(\d\d\d\d)/(\d\d)/(\d\d)/(.*?)/(.*?)$| )
        {
            # Get data
            (
                ${fileSize}, ${sourceName}, ${fileYear}, ${fileMonth},
                ${fileDay},  ${fileFormat}, ${fileName}
            ) = ( $1, $2, $3, $4, $5, $6, $7 );
        }
        else {
            if ( ${line} =~ m|^(\d+) (.*?)/(\d\d\d\d)/(\d\d)/(\d\d)/(.*?)$| ) {
                (
                    ${fileSize},  ${sourceName}, ${fileYear},
                    ${fileMonth}, ${fileDay},    ${fileName}
                ) = ( $1, $2, $3, $4, $5, $6 );
                ${fileFormat} = "NON-DEFINED";
            }
            else {
                if ( ${line} =~ m|^(\d+) (.*?)/(\d\d\d\d)/(\d\d)/(.*?)/(.*?)$| )
                {
                    (
                        ${fileSize},  ${sourceName}, ${fileYear},
                        ${fileMonth}, ${fileFormat}, ${fileName}
                    ) = ( $1, $2, $3, $4, $5, $6 );
                    ${fileDay} = "01";
                }
                else {
                    if ( ${line} =~ m|^(\d+) (.*?)/(.*?)$| ) {
                        ( ${fileSize}, ${sourceName}, ${fileName} ) =
                          ( $1, $2, $3 );

                        ${fileFormat} = "NON-DEFINED";
                        ${fileYear}   = "";
                        ${fileMonth}  = "";
                        ${fileDay}    = "";
                    }
                    else {
                        print "ERROR: $0: Can't match line: ${line}\n";
                        next;
                    }
                }
            }
        }

        my ${dateToWrite};
        if ( ${fileDay} ne "" ) {
            my ${dateFile} = DateTime->new(
                year   => ${fileYear},
                month  => ${fileMonth},
                day    => ${fileDay},
                hour   => 0,
                minute => 0,
                second => 0,
            );

            # Check last date for that source and format
            my $name = "${sourceName}/${fileFormat}";
            if ( defined $lastDatePerSource{ ${name} } ) {

                # Get date and check to add 0
                my $startDateSource = $lastDatePerSource{ ${name} };

                # Add 1 to $startDateSource, because we are waiting following day
                ${startDateSource}->add( hours => 24 );
                while ( ${startDateSource} < ${dateFile} ) {

                    # Get date with correct format to be written in file
                    my $dateToPrint =
                      $format->format_datetime($startDateSource);
                    print $outputFile
                      "${sourceName}|${fileFormat}|${dateToPrint}|NULL|0\n";

                    # Add interval time
                    ${startDateSource}->add( hours => 24 );
                }
            }

            # Update last date for that source
            $lastDatePerSource{ ${name} } = ${dateFile};
            ${dateToWrite} = "${fileYear}-${fileMonth}-${fileDay}";
        }
        else {
            if ( ${fileYear} eq "" ) {
                ${dateToWrite} = "";
            }
        }

        print $outputFile "${sourceName}|${fileFormat}|${dateToWrite}|${fileName}|${fileSize}\n";
    }

    close( ${inputFile} );
}

close( ${outputFile} );
exit 0
