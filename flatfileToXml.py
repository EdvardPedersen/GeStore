#!/usr/bin/python
import sys
import anydbm
import argparse
import math
from collections import defaultdict
import cProfile

NUM_PROGRESS_REPORTS = 100

MAX_ENTRIES = 10000
PRINT_ENTRY = 1000

ENTRIES_PER_PASS = 1000

class Entry:
    def __init__(self, query_id, subject_id, percent_identity, alignment_length, mismatch_count, gap_open_count, query_start, query_end, subject_start, subject_end, e_value, bit_score):
        self.query_id = query_id
        self.subject_id = subject_id
        self.percent_identity = percent_identity
        self.alignment_length = alignment_length
        self.mismatch_count = mismatch_count
        self.gap_open_count = gap_open_count
        self.query_start = query_start
        self.query_end = query_end
        self.subject_start = subject_start
        self.subject_end = subject_end
        self.e_value = e_value
        self.bit_score = bit_score
        self.query_length = str(int(self.query_end) - int(self.query_start))
        self.subject_length = str(int(self.subject_end) - int(self.subject_start))
        self.identity = str(int(self.query_length) - int(self.mismatch_count))
        self.positive = str(int(self.identity) - int(self.gap_open_count))
    
    def get_iteration(self):
        return {'query-def':self.query_id, 'query-len':str(self.query_length)}
    
    def get_hit(self):
        return {'def':self.subject_id, 'len':str(self.subject_length)}
    
    def get_hsp(self):
        return {'align-len':str(self.alignment_length), 'evalue':self.e_value, 'identity':str(self.identity), 'positive':str(self.positive), 'score':self.bit_score, 'query-from':self.query_start, 'query-to':self.query_end, 'hit-from':self.subject_start, 'hit-to': self.subject_end}
    
    # Return 1 for new iteration, 2 for new hit, 3 for new hsp, 0 for equal
    def compare(self, otherGuy):
        ourIter = self.get_iteration()
        ourHit = self.get_hit()
        ourHsp = self.get_hsp()
        
        theirIter = otherGuy.get_iteration()
        theirHit = otherGuy.get_hit()
        theirHsp = otherGuy.get_hsp()
        
        for key in ourIter:
            if ourIter[key] != theirIter[key]:
                return 1
        
        for key in ourHit:
            if ourHit[key] != theirHit[key]:
                return 2
                
        for key in ourHsp:
            if ourHsp[key] != theirHsp[key]:
                return 3
        return 0    
    
    def __str__(self):
        return self.query_id + "\t" + self.subject_id + "\t" + self.percent_identity + "\t" + self.alignment_length + "\t" + self.mismatch_count + "\t" + self.gap_open_count + "\t" + self.query_start + "\t" + self.query_end + "\t" + self.subject_start + "\t" + self.subject_end + "\t" + self.bit_score

def main(args):
    parser = argparse.ArgumentParser(description='Generate XML output from BLAST tabular format')
    # Input file
    parser.add_argument('-i', '--input', required=True, help='Tabular file to be parsed')
    parser.add_argument('-d', '--deleted', help='File containing deleted entries, seperated by newlines')
    parser.add_argument('-p', '--program', required=True, help='Name of the program used to generate input')
    parser.add_argument('-t', '--taxon', required=True, help='Taxonomy name')
    options = parser.parse_args(args)
    
    file1 = open(options.input, 'r')
    entries = dict()
    deleted = set()
    
    if(options.deleted):
      #delFile = open(options.deleted)
      if(os.path.isFile(options.deleted)):
	for line in open(options.deleted):
	  deleted.add(line.strip())
    
    last_entry = 0
    
    iteration_num = 0
    hit_num = 0
    hsp_num = 0
    
    print "<?xml version=\"1.0\"?>"
    print "<!DOCTYPE BlastOutput PUBLIC \"-//NCBI//NCBI BlastOutput/EN\" \"http://www.ncbi.nlm.nih.gov/dtd/NCBI_BlastOutput.dtd\">"
    print "<BlastOutput>"
    print "  <BlastOutput_program>" + options.program + "</BlastOutput_program>"
    print "  <BlastOutput_iterations>"
    
    for line in file1:
        #Needed for annotator: queryID, subjectID, identity, alnLength, 
        try:
            (queryId, subjectId, percIdentity, alnLength, mismatchCount, gapOpenCount, queryStart, queryEnd, subjectStart, subjectEnd, eVal, bitScore) = line.strip().split("\t")
        except:
            continue
        if(subjectId in deleted):
            continue
        tempEntry = Entry(queryId, subjectId, percIdentity, alnLength, mismatchCount, gapOpenCount, queryStart, queryEnd, subjectStart, subjectEnd, eVal, bitScore)
        if(last_entry):
            difference = tempEntry.compare(last_entry)
        else:
            difference = 1
        if difference == 3:
            continue
        iteration = tempEntry.get_iteration()
        hit = tempEntry.get_hit()
        hsp = tempEntry.get_hsp()
        if(difference == 1):
            if(iteration_num > 0):
                print "          </Hit_hsps>"
                print "        </Hit>"
                print "      </Iteration_hits>"
                print "    </Iteration>"
            iteration_num = iteration_num + 1
            hit_num = 0
            hsp_num = 0
            print "    <Iteration>"
            print "      <Iteration_iter-num>" + str(iteration_num) + "</Iteration_iter-num>"
            print "      <Iteration_query-def>" + iteration['query-def'] + "</Iteration_query-def>"
            print "      <Iteration_query-len>" + iteration['query-len'] + "</Iteration_query-len>"
            print "      <Iteration_hits>"
        
        if(difference > 0 and difference < 3):
            hit_num = hit_num + 1
            hsp_num = 0
            if(difference > 1):
                print "          </Hit_hsps>"
                print "        </Hit>"
            print "        <Hit>"
            print "          <Hit_num>" + str(hit_num) + "</Hit_num>"
            print "          <Hit_def>" + hit['def'] + " " + options.taxon + "</Hit_def>"
            print "          <Hit_len>" + hit['len'] + "</Hit_len>"
            print "          <Hit_hsps>"
        
        print "            <Hsp>"
        print "              <Hsp_num>" + str(hsp_num) + "</Hsp_num>"
        print "              <Hsp_align-len>" + hsp['align-len'] + "</Hsp_align-len>"
        print "              <Hsp_evalue>" + hsp['evalue'] + "</Hsp_evalue>"
        print "              <Hsp_identity>" + hsp['identity'] + "</Hsp_identity>"
        print "              <Hsp_positive>" + hsp['positive'] + "</Hsp_positive>"
        print "              <Hsp_score>" + hsp['score'] + "</Hsp_score>"
        print "              <Hsp_query-from>" + hsp['query-from'] + "</Hsp_query-from>"
        print "              <Hsp_query-to>" + hsp['query-to'] + "</Hsp_query-to>"
        print "              <Hsp_hit-from>" + hsp['hit-from'] + "</Hsp_hit-from>"
        print "              <Hsp_hit-to>" + hsp['hit-to'] + "</Hsp_hit-to>"
        print "            </Hsp>"
        hsp_num = hsp_num + 1
        last_entry = tempEntry
      
    print "          </Hit_hsps>"
    print "        </Hit>"
    print "      </Iteration_hits>"
    print "    </Iteration>"
    print "  </BlastOutput_iterations>"
    print "</BlastOutput>"



if __name__ == "__main__":
    main(sys.argv[1:])
