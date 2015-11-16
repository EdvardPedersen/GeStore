#!/usr/bin/python
import sys
import time
import os
#import argparse
from optparse import OptionParser
from collections import deque
from subprocess import call
import shlex

def main(args):
  home_folder = os.getenv('HOME')
  gestore_path = os.getenv('GESTORE_PATH', 'move.jar')
  usage = "%prog [options] file id"

  parser = OptionParser(usage, description="Add data to GeStore")
  parser.add_option('--run', '-r', help="Identifier for the pipeline being used")
  parser.add_option('--task', '-t', help="Identifier for the task of the pipeline")
  parser.add_option('--timestamp', '-e', help="The timestamp for this file")
  parser.add_option('--additional', '-a', help="Plugin-specific extra information")
  parser.add_option('--format', '-f', help="The plugin to use")
  parser.add_option('--conf', '-c', default=home_folder + '/gestore-conf.xml', help="Which configuration file to use, defaults to ~/gestore-conf.xml")

  (options, arguments) = parser.parse_args()
  if(len(arguments) < 2):
    print "Missing file ID or path!"
    parser.print_help()
    return
  
  output = "hadoop jar " + gestore_path + " org.gestore.move -Dfile_id=" + arguments[1] + " -Dpath=" + arguments[0]  
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
  call(shlex.split(output))
  
if __name__ == "__main__":
  main(sys.argv[1:])
