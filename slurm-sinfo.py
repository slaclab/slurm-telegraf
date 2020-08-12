#!/bin/env python

import sys
import logging
import re

# squeue -rh -o '%g %u %P %16b %T %C %D %R'

pattern = re.compile(
            r"(?P<partition>.*)\s(?P<mem>\d*)\s(?P<cpu>\d*)\s"
            r"(?P<features>.*)\s(?P<gres>.*)\s"
            r"(?P<state>[^*$~#]*)[*$~#]?\s(?P<nodecnt>\d*)\s"
            r"(?P<allocated>\d*)/(?P<idle>\d*)/(?P<other>\d*)/(?P<total>\d*)")

#transtable = str.maketrans('.', '_')
partitions = set()
data = {}

for line in sys.stdin:
 
  logging.info(f"> {line}")

  match = pattern.match(line)
  if match:
    partition = match.group("partition") #.translate(None, '*')
    partitions.add(partition)
    features = match.group("features") #.translate(transtable, '*')
    fields = []
    for i in features.split(','):
      k, v = i.split(':')
      fields.append( f"{k.lower()}={v.lower()}" )

    gres = match.group("gres")
    base_path=f"sinfo partition={partition},mem={match.group('mem')},cpu={match.group('cpu')},{','.join(fields)}"

    state = match.group("state")
    nodecnt = int(match.group("nodecnt"))

    if base_path not in data:
      data[base_path] = {'nodes_allocated': 0, 'nodes_completing': 0,
                                         'nodes_down': 0, 'nodes_drained': 0,
                                         'nodes_draining': 0, 'nodes_idle': 0,
                                         'nodes_maint':0, 'nodes_mixed': 0, 'nodes_unknown': 0, 'nodes_total': 0,
                          'cpus_allocated': 0, 'cpus_idle': 0, 'cpus_other': 0, 'cpus_total': 0,
      }
    data[base_path]['nodes_total'] += nodecnt
    try:
      data[base_path]['nodes_'+state] += nodecnt
    except KeyError as e:
      data[base_path]['nodes_'+state] = nodecnt

    data[base_path]['cpus_total'] += int(match.group('total'))
    for cpustate in ('allocated', 'idle', 'other'):
      data[base_path]['cpus_'+cpustate] += int(match.group(cpustate))

for base_path,item in data.items():
  values = []
  for k,v in item.items():
    values.append( f"{k}={v}" )
  print( f"{base_path} {','.join(values)}" )
  
