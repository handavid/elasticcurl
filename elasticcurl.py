#!/usr/bin/env python

import argparse
import json
import os
import re
import subprocess
import sys
import time

class ElasticCurl:

  def __init__(self, args):
    self.args    = args
    self.inurl   = args.input.find(":")
    self.outurl  = args.output.find(":")
    self.tmpfile = None
    self.tmpname = "/tmp/elasticcurl.json"

  def emit(self, line):
    print time.asctime(time.localtime(time.time())) + " | " + line

  def put_line(self, line):
    if self.outurl == -1: self.outfile.write(line)
    else:
      match = re.search(r'"_index":"([^"]*)"', line); _index = match.group(1)
      match = re.search(r'"_type":"([^"]*)"', line);  _type  = match.group(1)
      match = re.search(r'"_id":"([^"]*)"', line);    _id    = match.group(1)
      self.tmpfile.write("{\"index\":{\"_index\":\"" + _index + "\",\"_type\":\"" + _type + "\",\"_id\":\"" + _id + "\"}}\n")
      self.tmpfile.write(line)

  def get_lines_from_file(self, limit, offset):
    linesread = 0
    if self.outurl != -1: self.tmpfile = open(self.tmpname,'w')
    for num in range(0, limit):
      line = self.infile.readline()
      if line == "": break
      self.put_line(line)
      linesread += 1
    if self.outurl != -1: self.tmpfile.close()
    return linesread

  def get_lines_from_es(self, limit, offset):
    cmd = "curl -s \"" + args.input + "/_search?size=" + str(limit) + "&from=" + str(offset) + "\""
    result = json.loads(subprocess.check_output(cmd, shell=True))
    linesread = 0
    if self.outurl != -1: self.tmpfile = open(self.tmpname,'w')
    for line in result['hits']['hits']:
      self.put_line(json.dumps(line, sort_keys=True, separators=(',', ':')) + "\n")
      linesread += 1
    if self.outurl != -1: self.tmpfile.close()
    return linesread

  def put_lines_to_file(self, linesread):
    return linesread # the lines have already been written through put_line()

  def put_lines_to_es(self, linesread):
    cmd = "curl -s -XPOST " + self.args.output + "/_bulk --data-binary @" + self.tmpname
    result = json.loads(subprocess.check_output(cmd, shell=True))
    lineswrote = 0
    for line in result['items']:
      lineswrote += 1 # should probably check if result was 'ok'
    return lineswrote

  def get_lines(self, limit, offset):
    return self.get_lines_from_file(limit, offset) if self.inurl == -1 else self.get_lines_from_es(limit, offset)

  def put_lines(self, linesread):
    return self.put_lines_to_file(linesread) if self.outurl == -1 else self.put_lines_to_es(linesread)

  def run(self):
    self.emit("elasticcurl begin")

    self.infile  = None if  self.inurl != -1 else open(self.args.input)
    self.outfile = None if self.outurl != -1 else open(self.args.output,'w')

    linesin  = 0
    linesout = 0
    while True:
      linesread = self.get_lines(self.args.limit, linesin)
      if linesread == 0: break
      linesin += linesread
      self.emit("Read " + str(linesin) + " lines total")
      lineswrote = self.put_lines(linesread)
      linesout += lineswrote
      self.emit("Wrote " + str(linesout) + " lines total")

    if  self.inurl == -1:  self.infile.close()
    if self.outurl == -1: self.outfile.close()

    self.emit("elasticcurl end")

parser = argparse.ArgumentParser()
parser.add_argument('--input',  required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--limit',  type=int,  default=10000)
args = parser.parse_args()

curler = ElasticCurl(args)
curler.run()

