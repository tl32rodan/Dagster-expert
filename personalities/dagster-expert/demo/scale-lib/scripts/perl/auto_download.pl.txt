#!/usr/bin/env perl
# Generic mock perl step. ALL perl steps in scripts/perl/<step>.pl are
# symlinks to this file; the script name (basename minus .pl) is the
# step name. Real production scripts replace each symlink with a real
# binary; the demo just produces a couple of mock files per (lib,
# branch, step) so the folder-digest data-version contract has
# something to hash.
#
# CLI contract (see CONTRACT.md):
#   --library <name>  --branch <name>  --step <name>  --out <dir>
#
# Output: a couple of marker files in $out_dir. The Dagster runner
# computes the folder digest after this script returns 0.

use strict;
use warnings;
use Getopt::Long;
use File::Path qw(make_path);
use File::Basename qw(basename);

my ($library, $branch, $step, $out) = ('', '', '', '');
GetOptions(
    'library=s' => \$library,
    'branch=s'  => \$branch,
    'step=s'    => \$step,
    'out=s'     => \$out,
) or die "bad options";

die "missing --library" unless $library;
die "missing --branch"  unless $branch;
die "missing --step"    unless $step;
die "missing --out"     unless $out;

make_path($out) unless -d $out;

# Two mock outputs per run — one summary, one binary-ish stub.
open my $fh, '>', "$out/result.txt" or die "cannot write result.txt: $!";
print $fh "library=$library\nbranch=$branch\nstep=$step\nrunner=perl\n";
print $fh "timestamp=", scalar(time()), "\n";
close $fh;

open my $bin, '>', "$out/blob.bin" or die "cannot write blob.bin: $!";
# Deterministic mock blob — derived from (lib,branch,step) so re-runs
# produce identical bytes (data_version stable across reruns).
my $payload = "$library|$branch|$step";
print $bin $payload x 10;
close $bin;

exit 0;
