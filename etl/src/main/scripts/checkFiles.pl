#!/usr/bin/perl
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

# Get base directory and default properties file location
my $baseDir = cwd($0);
my $propertiesFileName = "$baseDir/properties/checkFiles.properties";

my $startDate = '2014-10-01';
my $endDate = '2014-10-13';

GetOptions ('propertiesFileName=s' => \$propertiesFileName,
    'startDate=s' => \$startDate,
    'endDate=s' => \$endDate);

# Open properties file for reading
open(my $propertiesFile, "<$propertiesFileName") or die "ERROR: $0: Can't open $propertiesFileName for reading: +$!\n";

# Load properties file
my $properties = Config::Properties->new();
$properties->load($propertiesFile);

# Get configuration from properties
my $directory = $properties->getProperty("directory")
    or die "ERROR: $0: Can't get property: directory from: ${propertiesFileName}: +$!\n";
my $filePattern = $properties->getProperty("filePattern")
    or die "ERROR: $0: Can't get property: filePattern from: ${propertiesFileName}: +$!\n";
my $outputPath = $properties->getProperty("outputPath")
    or die "ERROR: $0: Can't get property: outputPath from: ${propertiesFileName}: +$!\n";
my $filesIntervalMinutes = $properties->getProperty("filesIntervalMinutes")
    or die "ERROR: $0: Can't get property: filesIntervalMinutes from: ${propertiesFileName}: +$!\n";

# Close properties file
close($propertiesFile);

print "INFO: Parameters are:\n";
print " Directory to check: $directory \n";
print " File pattern is: $filePattern \n";
print " Start date to check: $startDate \n";
print " End date to check: $endDate \n";
print " Output path: $outputPath \n";
print " Interval between files: $filesIntervalMinutes \n";

my @files = <${directory}/*${filePattern}>;
my ($file, $fileSize);
my ($name, $path, $suffix);

my %files_size;
my $key;
foreach $file (@files) {
    ($name, $path, $suffix) = fileparse($file, $filePattern);
    $fileSize = -s $file;    
    
    # Check if we file already in memory
    if ( defined $files_size{ $name} ) {
        # If file exist add file size
        $files_size{$name} += $fileSize;
    } else {
        # Add to vector
        $files_size{$name} = $fileSize;
    }
} 

my ($year, $month, $day) = split '\-', $startDate;
my $dtStart =  DateTime->new( year => $year,
                          month      => $month,
                          day        => $day,
                          hour       => 0,
                          minute     => 0,
                          second     => 0,
                           );
($year, $month, $day) = split '\-', $endDate;
my $dtEnd =  DateTime->new( year => $year,
                          month      => $month,
                          day        => $day,
                          hour       => 0,
                          minute     => 0,
                          second     => 0,
                           );

my $format = DateTime::Format::Strptime->new(
    pattern   => '%d.%m.%Y_%H_%M',
    time_zone => 'local',
    on_error  => 'croak',
);
while($dtStart < $dtEnd){
    # Check if we have user in memory
    $name = $format->format_datetime($dtStart);
    if ( not defined $files_size{ $name } ) {
        # If not defined add file size as zero
        $files_size{$name} = 0;
        print "Not found: ", $format->format_datetime($dtStart), "\n";
    }
    
    $dtStart->add(minutes => $filesIntervalMinutes);
}
    
my $outputFileName = "${outputPath}/checkFiles_${startDate}_${endDate}.csv";
open(my $outputFile, ">${outputFileName}") or die "ERROR: $0: Can't open $outputFileName for writing : +$!\n";

# Complete with zeros when we have no file
my ($dayFile, $hourFile, $minuteFile);
foreach $key (sort keys %files_size) {
    ($dayFile, $hourFile, $minuteFile) = split '\_', $key;
    print $outputFile "${dayFile}|${hourFile}:${minuteFile}|${files_size{$key}}\n";
}

# Close output file
close($outputFile);
