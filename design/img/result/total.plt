#!/usr/bin/gnuplot
set term png
set output "total.png"
set xlabel "file system"
set ylabel "time in seconds"
set style fill solid
set boxwidth 0.5
plot "total.dat" using 2:xtic(1)         notitle with boxes, \
     ''          using 0:($2 + 10):($2) notitle with labels
