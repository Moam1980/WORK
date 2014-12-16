#!/usr/bin/perl

use strict;
use warnings;
use Compress::Bzip2;
use File::Basename;
use Getopt::Long;

# Disable buffering for STDOUT,STDERR (log)
my $old_fh = select(STDOUT);
$| = 1;
select(STDERR);
$| = 1;
select($old_fh);

# Check number of arguments
my $usage = "Usage: extractBzip2.pl --file <filename>\n";

# Mandatory arguments
my $fileName;
GetOptions( 'file=s' => \$fileName );

my $verbose = '';    # option variable with default value (false)
GetOptions( 'verbose!' => \$verbose );

die "ERROR: $0: ERROR: File name is mandatory\n $usage"
  if ( !defined $fileName );

sub printInfo {
    print "INFO: $0: @_\n";
}

sub printError {
    print "ERROR: $0: @_\n";
}

printInfo "Starting processing file: ${fileName}";
my $baseName = basename( $fileName, ".bz2" );
my $outputFileName = "${baseName}.csv";
open( my $file, ">", "${outputFileName}" )
  or die "ERROR: $0: Can't open $outputFileName for writing : +$!\n";

# Open file for reading
my $bz = bzopen( $fileName, "rb" )
  or die "ERROR: $0: Can't open $fileName for reading : +$!\n";

# Read all file
while ( $bz->bzreadline($_) > 0 ) {
    my ($line) = $_;
    chomp($line);

    my @values = split( '\|', $line );

    my $first = 0;
    foreach my $val (@values) {
        $val =~ s/^\s+|\s+$//g;
        if ( $first == 0 ) {
            print $file "$val";
            $first = 1;
        }
        else {
            print $file "|$val";
        }
    }

    print $file "\n";
}

# Close files
$bz->bzclose();
close($file);

# Finished
printInfo "Finished processing file: ${fileName} and generated ${fileName}.csv";
