#!/usr/bin/env python

import sys
import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument('--input',  required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--limit',  type=int,  default=100)
parser.add_argument('--offset', type=int,  default=0)
parser.add_argument('--debug',  type=bool, default=False)
args = parser.parse_args()

with open(args.input) as inputfile:
  with open("temp.json",'w') as outputfile:
    for num in range(0, args.limit):
      line = inputfile.readline()
      match = re.search(r'"_index":"([^"]*)","_type":"([^"]*)","_id":"([^"]*)"', line);
      _index = match.group(1)
      _type  = match.group(2)
      _id    = match.group(3)
      outputfile.write("{ \"index\" : { \"_index\" : \"" + _index + "\", \"_type\" : \"" + _type + "\", \"_id\" : \"" + _id + "\" } }\n")
      outputfile.write(line)

