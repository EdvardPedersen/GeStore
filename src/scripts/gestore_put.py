#!/usr/bin/python
import sys
import time
import os
#import argparse
from optparse import OptionParser
from collections import deque
from subprocess import call

def main(args):
  home_folder = os.getenv('HOME')
  usage = "%prog [options] file gestore_id"
  parser = OptionParser(usage, description="Add data to GeStore")
  #parser.add_option('--ID', help='The ID for the data entered into GeStore, used to access the data')
  parser.add_option('--run', '-r', help="Identifier for the pipeline being used")
  parser.add_option('--task', '-t', help="Identifier for the task of the pipeline")
  #parser.add_option('--start_time', '-b')
  parser.add_option('--timestamp', '-e', help="The timestamp for this file")
  parser.add_option('--additional', '-a', help="Plugin-specific extra information")
  #parser.add_option('--regex', '-x')
  #parser.add_option('--full_run', '-f', default=False, action="store_true")
  parser.add_option('--format', '-f', help="The plugin to use")
  parser.add_option('--conf', '-c', default=home_folder + '/gestore-conf.xml', help="Which configuration file to use, defaults to ~/gestore-conf.xml")
  #parser.print_help()
  (options, arguments) = parser.parse_args()
  if(len(arguments) < 2):
    print "Missing file ID!"
    parser.print_help()
    return
  
  #Hardcoded, hooray
  output = "hadoop jar /share/apps/gestore/move.jar org.diffdb.move -Dfile_id=" + arguments[1] + " -Dpath=" + arguments[0]
  
  if(options.run):
    output += " -Drun=" + options.run
    
  if(options.task):
    output += " -Dtask=" + options.task
  
  if(options.timestamp):
    output += " -Dtimestamp_stop=" + options.timestamp
  
  if(options.additional):
    output += " -Dtaxon=" + options.additional
  
  if(options.format):
    output += " -Dformat=" + options.format
  
  output += " -Dtype=l2r"
  
  output += " -conf=" + options.conf
  
  print output
  
if __name__ == "__main__":
  main(sys.argv[1:])