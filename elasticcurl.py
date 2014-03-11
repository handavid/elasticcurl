#!/usr/bin/env python

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import gzip

class ElasticCurl:

  def __init__(self, args):
    self.args    = args
    self.inurl   = args.input.find(":")   # if this is -1, it's not a URL (and therefore a file)
    self.outurl  = args.output.find(":")  # if this is -1, it's not a URL (and therefore a file)
    self.tmpfile = []
    self.scroll_id = ""

  def emit(self, line):
    print time.asctime(time.localtime(time.time())) + " | " + line
    sys.stdout.flush()

  def put_line(self, line):
    self.tmpfile.write(line)

  def get_items_from_file(self, limit, offset):
    itemsread = 0
    self.tmpfile = open(self.args.tmp,'w')
    for num in range(0, limit):
      coord = self.infile.readline()
      item  = self.infile.readline()
      if item == "": break
      self.put_line(coord)
      self.put_line(item)
      itemsread += 1
    self.tmpfile.close()
    return itemsread

  def get_chunk_from_es(self, limit, offset):
    return itemsread

  def get_items_from_es(self, limit, offset):
    self.tmpfile = open(self.args.tmp,'w')

    if self.args.scan:
      cmd = "curl -s -XGET '" + self.args.input + "/_search/scroll?scroll=10m' -d '" + self.scroll_id + "'"
    else:
      cmd = "curl -s \"" + self.args.input + "/_search?size=" + str(limit) + "&from=" + str(offset) + "\""
    result = json.loads(subprocess.check_output(cmd, shell=True))
    itemsread = 0
    for hit in result['hits']['hits']:
      _index = hit['_index'].replace("\"","\\\"")
      _type  = hit['_type' ].replace("\"","\\\"")
      _id    = hit['_id'   ].replace("\"","\\\"")
      self.put_line("{\"index\":{\"_index\":\"" + _index + "\",\"_type\":\"" + _type + "\",\"_id\":\"" + _id + "\"}}\n")
      self.put_line(json.dumps(hit['_source'], sort_keys=True, separators=(',', ':')) + "\n")
      itemsread += 1

    self.tmpfile = open(self.args.tmp,'w').close()
    return itemsread

  def put_items_to_file(self):
    itemswrote = 0
    thefile = open(self.args.tmp)
    while True:
      coord = thefile.readline()
      item  = thefile.readline()
      if item == "": break
      self.outfile.write(coord)
      self.outfile.write(item)
      itemswrote += 1
    thefile.close()
    return itemswrote

  def put_chunk_to_es(self, results):
    results[f] = itemswrote

  def put_items_to_es(self):
    itemswrote = 0
    cmd = "curl -s -XPOST " + self.args.output + "/_bulk --data-binary @" + self.args.tmp
    try:
      result = json.loads(subprocess.check_output(cmd, shell=True))
    except subprocess.CalledProcessError as e:
      self.emit("curl failed, error is " + str(e))
      sys.exit(1)
    for line in result['items']:
      if line.get('index'):
        if line['index'].get('ok'): itemswrote += 1
        else: print line
    return itemswrote

  def get_items(self, limit, offset):
    return self.get_items_from_file(limit, offset) if self.inurl == -1 else self.get_items_from_es(limit, offset)

  def put_items(self):
    return self.put_items_to_file() if self.outurl == -1 else self.put_items_to_es()

  def run(self):
    self.emit("elasticcurl begin")

    self.infile  = None if self.inurl  != -1 else (gzip.open( self.args.input     ) if  self.args.input.endswith("gz") else open( self.args.input     ))
    self.outfile = None if self.outurl != -1 else (gzip.open(self.args.output, 'w') if self.args.output.endswith("gz") else open(self.args.output, 'w'))

    if self.inurl != -1 and self.args.scan: # if we're reading from elasticsearch, initiate scan mode
      cmd = "curl -s -XGET '" + self.args.input + "/_search?search_type=scan&scroll=10m&size=" + str(self.args.limit) + "' -d '{ \"query\" : { \"match_all\" : {} } } '";
      result = json.loads(subprocess.check_output(cmd, shell=True))
      self.scroll_id = result['_scroll_id']

    itemsin  = 0
    itemsout = 0
    while True:
      itemsread = self.get_items(self.args.limit, itemsin)
      if itemsread == 0: break
      itemsin += itemsread
      self.emit("Read " + str(itemsin) + " items total")
      itemswrote = self.put_items()
      itemsout += itemswrote
      self.emit("Wrote " + str(itemsout) + " items total")

    if  self.inurl == -1:  self.infile.close()
    if self.outurl == -1: self.outfile.close()

    self.emit("elasticcurl end")

parser = argparse.ArgumentParser()
parser.add_argument('--input',  required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--limit',  type=int,  default=5000)
parser.add_argument('--scan',   type=bool, default=False)
parser.add_argument('--tmp',    default="/tmp/elasticcurl.json")
args = parser.parse_args()

curler = ElasticCurl(args)
curler.run()

