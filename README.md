elasticcurl
===========

Attempt at enabling elasticsearch snapshot / restore using python. Inspired by elasticdump, a similar Node.js application.

## Use

- Copy an index from production to staging: 
  - `elasticcurl.py --input http://production.es.com:9200/ --output http://staging.es.com:9200/`
- Backup an index to a file: 
  - `elasticcurl.py --input http://production.es.com:9200/ --output es.json`
- Backup ALL indices, then populate another ES cluster:
  - `elasticcurl.py --input http://staging.es.com:9200/ --output es.json`
  - `elasticcurl.py --input es.json --output http://production.es.com:9200/`

## Options

- `--input` (required) (see above)
- `--output` (required) (see above)
- `--limit` how many objects to move per batch (default: 10000)

