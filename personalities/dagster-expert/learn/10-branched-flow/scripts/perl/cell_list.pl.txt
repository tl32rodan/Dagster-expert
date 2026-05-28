#!/usr/bin/env perl
# cell_list.pl — library-agnostic cell list (shared across all libs).

use strict;
use warnings;
use Getopt::Long;

my $out_path;
GetOptions("out=s" => \$out_path) or die "usage: $0 --out <path>\n";
die "missing --out\n" unless $out_path;

my @cells = qw(INV BUF);   # smaller list for the demo

open(my $fh, '>', $out_path) or die "open $out_path: $!\n";
print $fh "[\n";
print $fh join(",\n", map { qq{  "$_"} } @cells);
print $fh "\n]\n";
close $fh;

print STDERR "cell_list.pl: wrote ", scalar(@cells), " cells -> $out_path\n";
exit 0;
