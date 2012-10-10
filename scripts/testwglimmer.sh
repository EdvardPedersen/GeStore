#!/bin/sh
#Starts Gepan
/opt/local/perl5122/bin/perl startGePan.pl -w /home/emr023/sekvensdata/Vironome/VSTall/ -f /home/emr023/sekvensdata/Vironome/VSTall/assembly/454LargeContigs.fna -p "glimmer3;blastp;pfam;priam" -T nucleotide -S contig -t "virus,all" -q 15 -o 3
