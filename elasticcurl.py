#!/usr/bin/env python

import sys
import argparse
import re
import os

parser = argparse.ArgumentParser()
parser.add_argument('--input',  required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--limit',  type=int,  default=10000)
args = parser.parse_args()

def get_lines_file(limit):
  global totalLines
  global inputfile
  print "Wrote " + str(totalLines) + " lines"
  with open("/tmp/elasticcurl.json",'w') as outputfile:
    for num in range(0, limit):
      line = inputfile.readline()
      if line == "":
        return num == 0
      if line.startswith(","):
        line = line[1:]
      totalLines += 1
      match = re.search(r'"_index":"([^"]*)","_type":"([^"]*)","_id":"([^"]*)"', line);
      if match == None:
        continue
      _index = match.group(1)
      _type  = match.group(2)
      _id    = match.group(3)
      outputfile.write("{ \"index\" : { \"_index\" : \"" + _index + "\", \"_type\" : \"" + _type + "\", \"_id\" : \"" + _id + "\" } }\n")
      outputfile.write(line)
  return True

def put_lines_es():
  os.system("curl -s -XPOST " + args.output + "/_bulk --data-binary @/tmp/elasticcurl.json > /dev/null")

totalLines = 0
inputfile = open(args.input)
while (get_lines_file(args.limit)):
  put_lines_es()

