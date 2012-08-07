#!/bin/sh
#Starts Gepan
/opt/local/perl5122/bin/perl startGePan.pl -w /home/emr023/temp/gepantest/ -f /home/emr023/gepantest/test.fas -p "glimmer3;blastp;pfam;priam" -T nucleotide -S contig -t "bacteria,all" -q 15 -o 3
