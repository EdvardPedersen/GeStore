#!/usr/bin/python
import sys
import time
import os
from optparse import OptionParser
from collections import deque
from subprocess import call
import shlex

def main(args):
  home_folder = os.getenv('HOME')
  gestore_path = os.getenv('GESTORE_PATH', 'move.jar')
  usage = "%prog [options] file"
  parser = OptionParser(usage, description="Get files from GeStore")

  parser.add_option('--run', '-r', help="Identifier for the pipeline being used")
  parser.add_option('--task', '-t', help="Identifier for the task of the pipeline")
  parser.add_option('--start_time', '-b', help="The beginning timestamp when making incremental meta-data collections manually")
  parser.add_option('--stop_time', '-e', help="The stop timestamp when making incremental meta-data collections manually")
  parser.add_option('--additional', '-d', help="Additional parameter sent to the plugin")
  parser.add_option('--regex', '-x', help="Selection of fields, format: field=regex, example: ID=.*")
  parser.add_option('--full_run', '-f', default=False, action="store_true", help="If set, do not attempt to generate an incremental update")
  parser.add_option('--conf', '-c', default=home_folder + '/gestore-conf.xml', help="Which configuration file to use, defaults to ~/gestore-conf.xml")

  (options, arguments) = parser.parse_args()
  if(len(arguments) < 1):
    print "Missing file ID!"
    parser.print_help()
    return
  output = "hadoop jar " + gestore_path + " org.gestore.move -Dfile=" + arguments[0]
  
  if(options.run):
    output += " -Drun=" + options.run
    
  if(options.task):
    output += " -Dtask=" + options.task
    
  if(options.start_time):
    output += " -Dtimestamp_start=" + options.start_time
  
  if(options.stop_time):
    output += " -Dtimestamp_stop=" + options.stop_time
  
  if(options.regex):
    output += " -Dregex=" + options.regex
  
  if(options.full_run):
    output += " -Dfull_run=true"

  if(options.additional):
    output += " -Dtaxon=" + options.additional
  
  output += " -Dtype=r2l"
  
  output += " -conf=" + options.conf
  
  print output
  call(shlex.split(output))
  
if __name__ == "__main__":
  main(sys.argv[1:])
