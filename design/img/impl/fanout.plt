#!/usr/bin/gnuplot
set term png
set output "fanout.png"
set logscale x 2
set xlabel "min fanout"
set ylabel "time in seconds"
set yrange [0:]
plot "fanout.dat" notitle with linespoints pt 6
