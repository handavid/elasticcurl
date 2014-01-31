#!/usr/bin/env python

import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--limit',  default=100)
parser.add_argument('--offset', default=0)
parser.add_argument('--debug',  default=False)
parser.add_argument('--delete', default=False)
parser.add_argument('--all',    default=False)
parser.add_argument('--bulk',   default=False)
parser.add_argument('--input',  default='http://127.0.0.1:9200/sourceIndex')
parser.add_argument('--output', default='http://127.0.0.1:9200/destinationIndex')
args = parser.parse_args()

print args
