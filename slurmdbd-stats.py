#!/bin/env python

import sys
import logging
import re

def kv_parse( line: str ):
  k, v = line.split(':')
  return ( k.replace(' ', '_').lower(), v.strip() )

def stats_parse( line: str ):
  #\(\s*(?P<id_num>\d+)\s+coount:(?P<count>\d)\s+")
  d = re.search(r"(?P<metric>.*)\s+\(\s*\d+\)\s+count:(?P<count>\d+)\s+ave_time:(?P<ave_time>\d+)\s+total_time:(?P<total_time>\d+)",line)
  if d:
    out = d.groupdict()
    out['metric'] = out['metric'].strip().lower()
    return out

section = None
data = {}

logging.basicConfig( level=logging.WARNING )

for line in sys.stdin:
 
  line = line.strip()
  logging.info(f"> {line}")

  if line.startswith("Internal DBD rollup last ran"):
    section = "internal_dbd"
  elif line.startswith("Cluster 's3df' rollup statistics"):
    section = "cluster_rollup"
  elif line.startswith("Remote Procedure Call statistics by message type"):
    section = "message_type"
  elif line.startswith("Remote Procedure Call statistics by user"):
    section = "message_user"

  if section and not section in data:
    data[section] = {}

  try:

    if section in ( 'internal_dbd', 'cluster_rollup' ):
      k, v = kv_parse( line )
      logging.info(f"    parsed as: {section}, {k}={v}")
      data[section][k] = v

    elif section in ( 'message_type', 'message_user' ):
      stat = stats_parse( line )
      logging.info(f"    parsed as: {section}, {stat}")
      m = stat['metric']
      if not m in data[section]:
        data[section][m] = stat
        #del data[section][m]['metric']

  except Exception as e:
    logging.debug(f"  ignored line: {e}")

for section in data.keys():
  if section in ( 'internal_dbd', 'cluster_rollup' ):
    values = [ f"{k}={data[section][k]}" for k in data[section].keys() ]
    print( f"sacctmgr_stats,section={section} {','.join(values)}" )
  elif section in ( 'message_type', 'message_user' ):
    for m in data[section].keys():
      values = [ f"{k}={data[section][m][k]}" for k in data[section][m].keys() if not k == 'metric']
      field = 'rpc' if section == 'message_type' else 'user'
      print( f"sacctmgr_stats,section={section},{field}={m} {','.join(values)}" )
