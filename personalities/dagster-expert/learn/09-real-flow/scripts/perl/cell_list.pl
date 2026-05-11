#!/usr/bin/env perl
# cell_list.pl — emit the standard-cell list as JSON.
# Step 0 of the AP characterization flow.
#
# Real TSMC equivalent: queries the SOS library to enumerate
# the cells targeted for characterization.

use strict;
use warnings;
use Getopt::Long;

my $out_path;
GetOptions("out=s" => \$out_path) or die "usage: $0 --out <path>\n";
die "missing --out\n" unless $out_path;

# Hard-coded for the lesson; production would query SOS.
my @cells = qw(INV BUF NAND2 MUX2);

open(my $fh, '>', $out_path) or die "open $out_path: $!\n";
print $fh "[\n";
print $fh join(",\n", map { qq{  "$_"} } @cells);
print $fh "\n]\n";
close $fh;

print STDERR "cell_list.pl: wrote ", scalar(@cells), " cells to $out_path\n";
exit 0;
