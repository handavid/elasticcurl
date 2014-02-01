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

inurl  = args.input.find(":")
outurl = args.output.find(":")

totalLines = 0
infile  = None if  inurl != -1 else open(args.input)
outfile = None if outurl != -1 else open(args.output,'w')
tmpfile = None

def put_line(line):
  global outurl
  global outfile
  global tmpfile
  if outurl == -1: outfile.write(line)
  else:
    match = re.search(r'"_index":"([^"]*)","_type":"([^"]*)","_id":"([^"]*)"', line);
    if match == None: return
    _index = match.group(1)
    _type  = match.group(2)
    _id    = match.group(3)
    tmpfile.write("{ \"index\" : { \"_index\" : \"" + _index + "\", \"_type\" : \"" + _type + "\", \"_id\" : \"" + _id + "\" } }\n")
    tmpfile.write(line)

def get_lines_from_file(limit, offset):
  global infile
  global tmpfile
  linesread = 0
  if outurl != -1: tmpfile = open("/tmp/elasticcurl.json",'w')
  for num in range(0, limit):
    line = infile.readline()
    if line == "": return linesread
    if line.startswith(","): line = line[1:]
    linesread += 1
    put_line(line)
  if outurl != -1: tmpfile.close()
  return linesread

def get_lines_from_es(limit):
  return

def put_lines_to_file():
  return  # the lines have already been written through put_line()

def put_lines_to_es():
  os.system("curl -s -XPOST " + args.output + "/_bulk --data-binary @/tmp/elasticcurl.json > /dev/null")

def get_lines(limit, offset):
  return get_lines_from_file(limit, offset) if inurl == -1 else get_lines_from_es(limit, offset)

def put_lines():
  put_lines_to_file() if outurl == -1 else put_lines_to_es()

offset = 0
while True:
  newlines = get_lines(args.limit, offset)
  if newlines == 0: break
  put_lines()
  offset += newlines
  print "Read " + str(offset) + " lines"

if  inurl == -1:  infile.close()
if outurl == -1: outfile.close()

