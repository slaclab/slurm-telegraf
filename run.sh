#!/bin/sh

DIR=$( dirname "${BASH_SOURCE[0]}" )
squeue -rh -O jobid,state,username,Partition,Account,Qos,NumTasks,tres-alloc:100,Reason:70 | python3 ${DIR}/slurm-squeue.py
sdiag | python3 ${DIR}/slurm-sdiag.py
sinfo -h -e -o '%R %m %c %f %G %T %D %C' | python3 ${DIR}/slurm-sinfo.py
sinfo -Nh -O NodeHost,StateLong,SocketCoreThread,CPUsState,CPUsLoad,Memory,AllocMem,FreeMem,Disk,Weight,Features:100,Gres:100,GresUsed:100,Reason:50 | sort | uniq | python3 ${DIR}/slurm-sinfo-node.py
sshare -ahlP | python3 ${DIR}/slurm-sshare.py

