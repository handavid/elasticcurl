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
    self.inurl   = args.input.find(":")  # if this is -1, it's not a URL (and therefore a file)
    self.outurl  = args.output.find(":") # if this is -1, it's not a URL (and therefore a file)
    self.tmpfile = None

  def emit(self, line):
    print time.asctime(time.localtime(time.time())) + " | " + line
    sys.stdout.flush()

  def put_line(self, line):
    if self.outurl == -1: self.outfile.write(line)
    else:                 self.tmpfile.write(line)

  def get_items_from_file(self, limit, offset):
    itemsread = 0
    if self.outurl != -1: self.tmpfile = open(self.args.tmp,'w')
    for num in range(0, limit):
      coord = self.infile.readline()
      item  = self.infile.readline()
      if item == "": break
      self.put_line(coord)
      self.put_line(item)
      itemsread += 1
    if self.outurl != -1: self.tmpfile.close()
    return itemsread

  def get_items_from_es(self, limit, offset):
    cmd = "curl -s \"" + args.input + "/_search?size=" + str(limit) + "&from=" + str(offset) + "\""
    result = json.loads(subprocess.check_output(cmd, shell=True))
    itemsread = 0
    if self.outurl != -1: self.tmpfile = open(self.args.tmp,'w')
    for hit in result['hits']['hits']:
      self.put_line("{\"index\":{\"_index\":\"" + hit['_index'] + "\",\"_type\":\"" + hit['_type'] + "\",\"_id\":\"" + hit['_id'] + "\"}}\n")
      self.put_line(json.dumps(hit['_source'], sort_keys=True, separators=(',', ':')) + "\n")
      itemsread += 1
    if self.outurl != -1: self.tmpfile.close()
    return itemsread

  def put_items_to_file(self, itemsread):
    return itemsread # the items have already been written through put_line()

  def put_items_to_es(self, itemsread):
    cmd = "curl -s -XPOST " + self.args.output + "/_bulk --data-binary @" + self.args.tmp
    try:
      result = json.loads(subprocess.check_output(cmd, shell=True))
    except subprocess.CalledProcessError as e:
      self.emit("curl failed, error is " + str(e))
      sys.exit(1)
    itemswrote = 0
    for line in result['items']:
      itemswrote += 1 # should probably check if result was 'ok'
    return itemswrote

  def get_items(self, limit, offset):
    return self.get_items_from_file(limit, offset) if self.inurl == -1 else self.get_items_from_es(limit, offset)

  def put_items(self, itemsread):
    return self.put_items_to_file(itemsread) if self.outurl == -1 else self.put_items_to_es(itemsread)

  def run(self):
    self.emit("elasticcurl begin")

    self.infile  = None if  self.inurl != -1 else open(self.args.input)
    self.outfile = None if self.outurl != -1 else open(self.args.output,'w')

    itemsin  = 0
    itemsout = 0
    while True:
      itemsread = self.get_items(self.args.limit, itemsin)
      if itemsread == 0: break
      itemsin += itemsread
      self.emit("Read " + str(itemsin) + " items total")
      itemswrote = self.put_items(itemsread)
      itemsout += itemswrote
      self.emit("Wrote " + str(itemsout) + " items total")

    if  self.inurl == -1:  self.infile.close()
    if self.outurl == -1: self.outfile.close()

    self.emit("elasticcurl end")

parser = argparse.ArgumentParser()
parser.add_argument('--input',  required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--limit',  type=int,  default=10000)
parser.add_argument('--tmp',    default="/tmp/elasticcurl.json")
args = parser.parse_args()

curler = ElasticCurl(args)
curler.run()

