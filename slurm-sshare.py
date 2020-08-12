#!/bin/env python

import sys
import logging
import re

# squeue -rh -o '%g %u %P %16b %T %C %D %R'
assoc_tree_path = []


for line in sys.stdin:
 
   #logging.info(f"> {line}")

  fields = line.split('|')

  if len(fields) == 11:
    account, user, raw_shares, norm_shares, raw_usage, norm_usage, \
                eff_usage, fairshare, level_fs, grpcpumins, \
                cpurunmins = fields
  elif len(fields) == 10:
    level_fs = None
    account, user, raw_shares, norm_shares, raw_usage, norm_usage, \
                eff_usage, fairshare, grpcpumins, cpurunmins = fields
  else:
    logging.error(f"could not parse {line}")
    raise Exception("parsing failed")

  level = account.count(' ')
  if len(user) == 0:
    if level >= len(assoc_tree_path):
      assert level == len(assoc_tree_path)
      assoc_tree_path.append(account.strip())
    else:
      assoc_tree_path[level] = account.strip()
      assoc_tree_path = assoc_tree_path[:level + 1]

    if level_fs:
      if level_fs is 'inf':
        level_fs = sys.maxint
      level_fs = float(level_fs)
      #print( "sshare level_fs=%s share=%f" % ('.'.join(assoc_tree_path), level_fs  ))
  else:
    if level_fs:
      if level_fs is 'inf':
        level_fs = sys.maxint
      level_fs = float(level_fs)
      #print( "sshare level_fs=%s.%s share=%f" % ('.'.join(assoc_tree_path), user, level_fs ) )
    #assert( fairshare )
    #print( "sshare fairshare=%s.%s fairshare=%s" %  ('.'.join(assoc_tree_path), user, fairshare ) )

  if '/'.join(assoc_tree_path) != 'root':

    leaf=f',user={user}' if not len(user) == 0 else ''
    values = []
    if fairshare:
      values.append( f'fairshare={fairshare}' )
    if str(level_fs) != 'inf':
      values.append( f'share={level_fs}' )
    if len( values ):
      print( f"sshare,treepath={'/'.join(assoc_tree_path)}{leaf} {','.join(values)}" )
