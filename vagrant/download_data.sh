#!/usr/bin/env bash

mkdir test_data
cd test_data
wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/previous_releases/release-2015_01/knowledgebase/uniprot_sprot-only2015_01.tar.gz -q
tar -zxf uniprot_sprot-only2015_01.tar.gz
gunzip uniprot_sprot.dat.gz
cd ..
