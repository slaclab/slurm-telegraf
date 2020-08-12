


#squeue -rh -o '%g %u %P %16b %T %C %D %R' | python3 slurm-squeue.py
squeue -rh -O state,username,Partition,Account,Qos,NumTasks,tres-alloc:70,Reason:70 | python3 slurm-squeue.py
echo
sdiag | python3 slurm-sdiag.py
echo
sinfo -h -e -o '%R %m %c %f %G %T %D %C' | python3 slurm-sinfo.py
echo
echo
sinfo -Nh -O NodeHost,StateLong,SocketCoreThread,CPUsState,CPUsLoad,Memory,AllocMem,FreeMem,Disk,Weight,Features:100,Gres:100,GresUsed:100,Reason:50 | sort | uniq | python3 slurm-sinfo-node.py
sshare -ahlP | python3 slurm-sshare.py

