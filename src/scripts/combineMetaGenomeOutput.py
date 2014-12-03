#!/usr/bin/python
import sys
import anydbm
import argparse
import math
from collections import defaultdict
import cProfile
import os

NUM_PROGRESS_REPORTS = 100

MAX_ENTRIES = 10000
PRINT_ENTRY = 1000

ENTRIES_PER_PASS = 1000

def main(args):
    parser = argparse.ArgumentParser(description='Combine several fasta files from MG-RAST')
    # Input file
    parser.add_argument('-i', '--input', required=True, help='Root directory of files')
    parser.add_argument('-o', '--output', required=True, help='Output file')
    options = parser.parse_args(args)
    
    outputFile = open(options.output, 'w')
    
    bpcounter = 0
    filecounter = 0
    contigcounter = 0
    file_bp_counts = dict()

    for (dirpath, dirnames, filenames) in os.walk(options.input):
        #bpcounter = 0
        for filename in filenames:
            tempFile = os.path.join(dirpath, filename)
            if tempFile[-3:] == "fna":
                filecounter += 1
                file_bp_counts[tempFile] = 0
                for line in open(tempFile, 'r'):
                    outputFile.write(line)
                    if line[0:1] != ">":
                        bpcounter += len(line.strip())
                        file_bp_counts[tempFile] += len(line.strip())
                    else:
                        contigcounter += 1
                                
    print "Got " + str(bpcounter) + " base pairs in " + str(contigcounter) + " contigs from " + str(filecounter) + " files"
    print "Average contig length: " + str(bpcounter/contigcounter)
    combined = 0
    for entry in sorted(file_bp_counts.keys()):
        print "File " + entry + " had " + str(file_bp_counts[entry]) + " base pairs"



if __name__ == "__main__":
    main(sys.argv[1:])
