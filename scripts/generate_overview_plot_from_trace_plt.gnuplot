# Higher DPI Plots:
# https://unix.stackexchange.com/a/617970
dpi = 300 ## dpi (variable)
width = 250 ## mm (variable)
height = 400 ## mm (variable)

in2mm = 25.4 # mm (fixed)
pt2mm = 0.3528 # mm (fixed)

mm2px = dpi/in2mm
ptscale = pt2mm*mm2px
round(x) = x - floor(x) < 0.5 ? floor(x) : ceil(x)
wpx = round(width * mm2px)
hpx = round(height * mm2px)

set terminal pngcairo size wpx,hpx fontscale ptscale linewidth ptscale pointscale ptscale

set multiplot layout 5,1

set palette maxcolors 2

set lmargin at screen 0.1
set rmargin at screen 0.9
set xrange [XMIN:XMAX]
unset xtics
set key autotitle columnheader

set title "Task Offloads"
plot for [i=0:*] offloads index i using 1:2:3:4:yticlabels(2) with vectors notitle
set title "Utilization in the Moment of Offload"
plot for [i=0:*] offloads index i using 1:5 with impulses title columnheader(1)
set title "Task Runtime"
plot for [i=0:*] timings index i using 1:3 with impulses title columnheader(1)
set title "Runner Workload"
plot for [i=0:*] utilization index i using 1:3 with lines lw 2 title columnheader(1)
set title "Sent kb/s"
set logscale y
plot for [i=0:*] network_send index i using 1:2 with lines title columnheader(1)
unset logscale

unset multiplot
