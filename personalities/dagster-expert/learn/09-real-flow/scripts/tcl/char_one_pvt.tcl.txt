#!/usr/bin/env tclsh
# char_one_pvt.tcl — mock characterization driver.
#
# Step 4's actual EDA tool invocation, just mocked.
# Real TSMC equivalent: a Cadence Liberate / Synopsys SiliconSmart
# script that reads spice models + netlist and produces a .lib.
#
# Args:
#   1. corner (e.g. "ff")
#   2. vt    (e.g. "0p9v__25c")
#   3. out   (e.g. "/tmp/dagster-09-flow/liberate/ff__0p9v__25c.lib")

if {$argc != 3} {
    puts stderr "usage: $argv0 <corner> <vt> <out_path>"
    exit 1
}

set corner [lindex $argv 0]
set vt     [lindex $argv 1]
set out    [lindex $argv 2]

# Mock work: ~150ms (real char takes 30+ minutes per PVT).
after 150

# Write a tiny Liberty-shaped output.
set fh [open $out w]
puts $fh "library ($corner.$vt) \{"
puts $fh "  /* mock liberty for $corner $vt */"
puts $fh "  /* produced by char_one_pvt.tcl */"
puts $fh "  technology (cmos);"
puts $fh "  delay_model : table_lookup;"
puts $fh "  voltage_unit : \"1V\";"
puts $fh "  time_unit : \"1ns\";"
puts $fh "  cell (INV) \{"
puts $fh "    /* timing tables omitted in mock */"
puts $fh "  \}"
puts $fh "  cell (BUF) \{}"
puts $fh "  cell (NAND2) \{}"
puts $fh "  cell (MUX2) \{}"
puts $fh "\}"
close $fh

puts "char_one_pvt.tcl: $corner/$vt -> $out"
exit 0
