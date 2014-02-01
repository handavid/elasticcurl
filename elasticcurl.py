#!/usr/bin/env python

import argparse
import json
import os
import re
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--input',  required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--limit',  type=int,  default=10000)
args = parser.parse_args()

inurl  = args.input.find(":")
outurl = args.output.find(":")

infile  = None if  inurl != -1 else open(args.input)
outfile = None if outurl != -1 else open(args.output,'w')
tmpfile = None
tmpname = "/tmp/elasticcurl.json"

def emit(line):
  print time.asctime(time.localtime(time.time())) + " | " + line

def put_line(line):
  global outurl
  global outfile
  global tmpfile
  if outurl == -1: outfile.write(line)
  else:
    match = re.search(r'"_index":"([^"]*)"', line); _index = match.group(1)
    match = re.search(r'"_type":"([^"]*)"', line);  _type  = match.group(1)
    match = re.search(r'"_id":"([^"]*)"', line);    _id    = match.group(1)
    tmpfile.write("{\"index\":{\"_index\":\"" + _index + "\",\"_type\":\"" + _type + "\",\"_id\":\"" + _id + "\"}}\n")
    tmpfile.write(line)

def get_lines_from_file(limit, offset):
  global infile
  global tmpfile
  linesread = 0
  if outurl != -1: tmpfile = open(tmpname,'w')
  for num in range(0, limit):
    line = infile.readline()
    if line == "": break
    put_line(line)
    linesread += 1
  if outurl != -1: tmpfile.close()
  return linesread

def get_lines_from_es(limit, offset):
  cmd = "curl -s \"" + args.input + "/_search?size=" + str(limit) + "&from=" + str(offset) + "\""
  result = json.loads(subprocess.check_output(cmd, shell=True))
  linesread = 0
  if outurl != -1: tmpfile = open(tmpname,'w')
  for line in result['hits']['hits']:
    put_line(json.dumps(line, sort_keys=True, separators=(',', ':')) + "\n")
    linesread += 1
  if outurl != -1: tmpfile.close()
  return linesread

def put_lines_to_file(linesread):
  return linesread # the lines have already been written through put_line()

def put_lines_to_es(linesread):
  cmd = "curl -s -XPOST " + args.output + "/_bulk --data-binary @" + tmpname
  result = json.loads(subprocess.check_output(cmd, shell=True))
  lineswrote = 0
  for line in result['items']:
    lineswrote += 1 # should probably check if result was 'ok'
  return lineswrote

def get_lines(limit, offset):
  return get_lines_from_file(limit, offset) if inurl == -1 else get_lines_from_es(limit, offset)

def put_lines(linesread):
  return put_lines_to_file(linesread) if outurl == -1 else put_lines_to_es(linesread)

emit("elasticcurl begin")

linesin  = 0
linesout = 0
while True:
  linesread = get_lines(args.limit, linesin)
  if linesread == 0: break
  linesin += linesread
  emit("Read " + str(linesin) + " lines total")
  lineswrote = put_lines(linesread)
  linesout += lineswrote
  emit("Wrote " + str(linesout) + " lines total")

if  inurl == -1:  infile.close()
if outurl == -1: outfile.close()

emit("elasticcurl end")

