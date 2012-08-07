#!/bin/sh
#Starts Gepan
/opt/local/perl5122/bin/perl startGePan.pl -w /home/emr023/gepantest/ -f /home/emr023/temp/20000testmodified.fas -p "null;blastn;megan" -T nucleotide -S read -t "bacteria,all" -q 25 -o 3
