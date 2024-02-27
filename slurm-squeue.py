#!/bin/env python

import sys
import logging
import re

running = {}
other = {}

for line in sys.stdin:
  #logging.error(f"\n> {line}")
  this = {}

  try:
    out = line.strip().split('|')

    this['jobid'] = out.pop(0)
    this['state'] = out.pop(0)
    this['user'] = out.pop(0)
    this['partition'] = out.pop(0).replace(',', '_').replace('\ ', '')
    this['account'] = out.pop(0)
    this['qos'] = out.pop(0)
    if '^' in this['qos']:
        this['qos'] = this['qos'].split('^')[1]
        if '@' in this['qos']:
            this['qos'] = this['qos'].split('@')[0]
    #this will give an exception if ^ or @ is the last element of the string.

    this['tasks'] = out.pop(0)
    tres = out.pop(0)

    this['reason'] = str(' '.join(out)).replace(' ','\ ')

    if ', ' in this['reason'] or ',\ ' in this['reason']:
      this['reason'] = this['reason'].split(',').pop(0)
    if 'UnavailableNodes:' in this['reason']:
      this['reason'] = this['reason'].split( 'UnavailableNodes:' )[0]

    for i in tres.split(','):
      k, v = i.split('=')
      if k.startswith('gres/'):
        k = k.replace('gres/','')
      this[k] = v

    # normalise mem
    if 'mem' in this:
      multi = 1
      unit = this['mem'][-1]
      if unit == 'T':
        multi = 1024 * 1024
      elif unit == 'G':
        multi = 1024
      elif unit == 'M': #if this['mem'][-1] == 'M'
        multi = 1
      else:
        raise Exception(f"Unknown memory unit {unit}")
      v = float(this['mem'][:-1])
      this['mem'] = int(v * multi)

    #logging.error( f" -> {this}" )
  except ValueError as e:
    logging.warn( f"Could not parse {e}: {line}" )
    continue

  if this['state'] == 'RUNNING':
    key = (this['state'], this['user'], this['partition'], this['account'], this['qos'])
    if not key in running:
      running[key] = { 'jobs':0, 'cpu':0, 'gpu':0, 'mem':0, 'tasks':0, 'billing':0}
    running[key]['jobs'] += 1
    values = []
    for k in ( 'cpu', 'gpu', 'billing', 'mem', 'tasks' ):
      if k in this:
        running[key][k] += int(this[k])
        values.append( f"{k}={this[k]}i" )
    # TODO: too many unique tags
    #print( f"squeue-running,jobid={this['jobid']},user={this['user']},partition={this['partition']},account={this['account']},qos={this['qos']} {','.join(values)}" )

  else:
    key = (this['state'], this['reason'], this['user'], this['partition'], this['account'], this['qos'])
    if not key in other:
      other[key] = { 'jobs':0, 'cpu':0, 'gpu':0, 'mem':0, 'tasks':0, 'billing':0 }
    other[key]['jobs'] += 1
    values = []
    for k in ( 'cpu', 'gpu', 'billing', 'mem', 'tasks' ):
      if k in this:
        other[key][k] += int(this[k])
        values.append( f"{k}={this[k]}i" )
    # TODO: too many unique tags
    # print( f"squeue-pending,jobid={this['jobid']},reason={this['reason'].replace(' ', '_')},user={this['user']},partition={this['partition']},account={this['account']},qos={this['qos']} {','.join(values)}")


for (state, user, partition, account, qos), data in running.items():
    values = [ f"{k}={data[k]}i" for k in data.keys() ]
    print( f"squeue,user={user},partition={partition},account={account},qos={qos},state={state} {','.join(values)}" )

for (state, reason, user, partition, account, qos), data in other.items():
    values = [ f"{k}={data[k]}i" for k in data.keys() ]
    this_reason = reason.replace('\ ', '')
    print( f"squeue,user={user},partition={partition},account={account},qos={qos},state={state},reason={this_reason} {','.join(values)}" )
