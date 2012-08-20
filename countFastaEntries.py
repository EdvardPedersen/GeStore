#!/usr/bin/python
import sys
import math

def main(args):
    file1 = open(args[0], 'r')
    
    count = 0

    for line in file1:
        if line.startswith(">"):
            count += 1

    print count



if __name__ == "__main__":
    main(sys.argv[1:])
