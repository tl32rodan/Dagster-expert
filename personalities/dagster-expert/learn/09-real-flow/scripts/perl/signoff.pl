#!/usr/bin/env perl
# signoff.pl — assemble final sign-off package.
# Step 6 of the AP characterization flow.
#
# Real TSMC equivalent: tar the per-corner aggregates, attach
# manifest, push to ClioSoft SOS shelf for sign-off review.

use strict;
use warnings;
use Getopt::Long;

my ($aggregate_dir, $out_path);
GetOptions(
    "aggregate-dir=s" => \$aggregate_dir,
    "out=s"           => \$out_path,
) or die "usage: $0 --aggregate-dir DIR --out FILE\n";

die "missing --aggregate-dir\n" unless $aggregate_dir;
die "missing --out\n" unless $out_path;

opendir(my $d, $aggregate_dir) or die "opendir $aggregate_dir: $!\n";
my @lib_files = sort grep { /__merged\.lib$/ } readdir($d);
closedir $d;

die "no aggregated .lib files found under $aggregate_dir\n"
    unless @lib_files;

# Mock tar: write a manifest + concatenate all aggregates with separators.
open(my $fh, '>', $out_path) or die "open $out_path: $!\n";
print $fh "#! signoff package — produced by signoff.pl\n";
print $fh "#! corners=", scalar(@lib_files), "\n";
for my $f (@lib_files) {
    my $full = "$aggregate_dir/$f";
    print $fh "\n=== $f ===\n";
    open(my $in, '<', $full) or die "open $full: $!\n";
    while (my $line = <$in>) { print $fh $line; }
    close $in;
}
close $fh;

print STDERR "signoff.pl: packaged ", scalar(@lib_files), " corners -> $out_path\n";
exit 0;
