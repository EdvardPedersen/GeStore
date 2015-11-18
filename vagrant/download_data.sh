#!/usr/bin/env bash

mkdir test_data
cd test_data
mkdir 201501
mkdir 201502
mkdir 201503
wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/previous_releases/release-2015_01/knowledgebase/uniprot_sprot-only2015_01.tar.gz -q
tar -zxf uniprot_sprot-only2015_01.tar.gz
gunzip uniprot_sprot.fasta.gz
mv uniprot_sprot.fasta 201501/
wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/previous_releases/release-2015_02/knowledgebase/uniprot_sprot-only2015_02.tar.gz -q
tar -zxf uniprot_sprot-only2015_02.tar.gz
gunzip uniprot_sprot.fasta.gz
mv uniprot_sprot.fasta 201502/
wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/previous_releases/release-2015_03/knowledgebase/uniprot_sprot-only2015_03.tar.gz -q
tar -zxf uniprot_sprot-only2015_03.tar.gz
gunzip uniprot_sprot.fasta.gz
mv uniprot_sprot.fasta 201503/
rm *
cd ..
