#!/bin/sh
#Starts Gepan
/opt/local/perl5122/bin/perl startGePan.pl -w /home/emr023/gepantest/ -f /home/emr023/gepantest/test.fas -p "glimmer3;pfam;blastp;blastn;priam" -T nucleotide -S contig -t "bacteria,all" -q 10 -o 3
