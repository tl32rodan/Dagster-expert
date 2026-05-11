#!/usr/bin/env perl
# lpe.pl — Layout Parasitic Extraction per (corner, vt, cell).
# Step 1 of the AP characterization flow.
#
# Real TSMC equivalent: invokes Cadence Quantus or similar via
# its own batch interface. This mock writes a small .spef file
# (Standard Parasitic Exchange Format) named after the partition.

use strict;
use warnings;
use Getopt::Long;
use Time::HiRes qw(usleep);

my ($corner, $vt, $cell, $cell_list, $out_path);
GetOptions(
    "corner=s"    => \$corner,
    "vt=s"        => \$vt,
    "cell=s"      => \$cell,
    "cell-list=s" => \$cell_list,
    "out=s"       => \$out_path,
) or die "usage: $0 --corner X --vt Y --cell Z --cell-list cells.json --out file.spef\n";

for my $required ([$corner, "--corner"], [$vt, "--vt"], [$cell, "--cell"],
                  [$cell_list, "--cell-list"], [$out_path, "--out"]) {
    die "missing $required->[1]\n" unless defined $required->[0];
}

# Verify the cell is in the cell list (sanity check on upstream).
open(my $cl, '<', $cell_list) or die "cant open cell list $cell_list: $!\n";
my $cl_content = do { local $/; <$cl> };
close $cl;
die "cell $cell not in cell list\n" unless $cl_content =~ /\Q"$cell"\E/;

# Mock work: ~50ms per partition. Real LPE would take minutes.
usleep(50_000);

# Write .spef-shaped output (truncated mock).
open(my $fh, '>', $out_path) or die "open $out_path: $!\n";
print $fh "*SPEF \"IEEE 1481-1999\"\n";
print $fh "*DESIGN \"$cell\"\n";
print $fh "*DATE \"mock-${corner}-${vt}\"\n";
print $fh "*VENDOR \"Dagster lesson 09 mock\"\n";
print $fh "// extracted with mock LPE for ($corner, $vt, $cell)\n";
print $fh "// real .spef contains R/C network for the cell\n";
close $fh;

print STDERR "lpe.pl: $corner/$vt/$cell -> $out_path\n";
exit 0;
