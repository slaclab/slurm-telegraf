#!/bin/env python

import sys
import logging
import re

data = {}

def parse_gres( gres ):
  greses = {}
  for i in gres.split(','):
    if i.startswith('gpu'):
      a = None
      b = None
      if '(' in i:
        a,b = i.split('(')
      else:
        a = i
      try:
        form, model, num = a.split(':')
        if not form in greses:
          greses[form] = {}
        greses[form][model] = num
      except:
        pass

  for f in greses.keys():
    total = 0
    for k,v in greses[f].items():
      total += int(v)
    yield f, total
      
  
totals = {}

for line in sys.stdin:
 
  #logging.error(f"> {line}")

  this = {}
  item = line.split()

  this['nodename'] = item.pop(0)
  fields = [ f"nodename={this['nodename']}", ]

  this['state'] = item.pop(0).replace('*','')

  # this['sct'] = item.pop(0)
  this['sockets'], this['cores'], this['threads'] = item.pop(0).split(':')

  #this['cpus'] = item.pop(0)
  this['cpu_allocated'], this['cpu_idle'], this['cpu_other'], this['cpu_total'] = item.pop(0).split('/')
  this['cpu_load'] = item.pop(0)

  this['mem'] = item.pop(0)
  this['mem_allocated'] = item.pop(0)
  this['mem_free'] = item.pop(0)

  this['disk'] = item.pop(0)

  this['weight'] = item.pop(0)
  features = item.pop(0)
  if ',' in features:
    for i in features.split(','):
      k, v = i.split(':')
      fields.append( f"{k.lower()}={v.lower()}" )

  for gres, number in parse_gres( item.pop(0) ):
    this[gres + '_total'] = number
  for gres, number in parse_gres( item.pop(0) ):
    this[gres + '_allocated'] = number

  this['reason'] = ' '.join(item)

  #logging.error(f"=> {this}")

  if not this['state'] in totals:
    totals[this['state']] = { 'count': 0 }
  totals[this['state']]['count'] += 1

  values = []
  for i in ( 'cpu_allocated', 'cpu_idle', 'cpu_other', 'cpu_total', 'mem_allocated', 'mem_free', 'disk', 'weight' ):
    if this[i] == 'N/A':
      continue
    values.append(f'{i}={this[i]}i')
    # add tallies
    if not i in totals[this['state']]:
      totals[this['state']][i] = 0
    totals[this['state']][i] += int(this[i])

  values.append( f"state=\"{this['state']}\"" )
  if this['cpu_load'] != 'N/A':
    values.append( f"cpu_load={this['cpu_load']}" )

  if 'gpu_total' in this:
    for i in ( 'gpu_total', 'gpu_allocated' ):
      values.append( f"{i}={this[i]}i" )
      if not i in totals[this['state']]:
        totals[this['state']][i] = 0
      totals[this['state']][i] += int(this[i])
    idle = int(this['gpu_total']) - int(this['gpu_allocated'])
    values.append( f"gpu_idle={idle}i" )
    if not 'gpu_idle' in totals[this['state']]:
      totals[this['state']]['gpu_idle'] = 0
    totals[this['state']]['gpu_idle'] += idle

  print( f"sinfo-node,{','.join(fields)} {','.join(values)}" )

# print summary
for s in totals.keys():
  values = [ f"{k}={v}i" for k,v in totals[s].items() ]
  print( f"sinfo-state,state={s} {','.join( values )}" )
