import subprocess
import json
import copy
import time
import argparse
import sys
import traceback
"""
# Keys Tags that shoulg be monitored:
{
    account:,
    accrue_time:, #Time jobs has been running
    batch_host:, #TO-DO. Check if its the same value as nodes []
    contiguous:, # contiguous nodes request by the jobs (usefull to track the
                    difference in performance if it is used for jobs with
                    multiple tasks / vs / single tasks?
    group_id:,
    job_id:,
    job_resources: { nodes, allocated_cpus, allocated_hosts } #  Shows how good
                    the assignment of job resources was for this job. Usefull to
                    check if the allocated_hosts could be improved by the
                    "scheduler". And shows unassigned cores/memory from the
                    allocates_host that is not been used by the Job. Usefull to
                    track number of unassigned cores.
    job_state:,
    cpus:,
    node_count:,
    tasks:, #Number of tasks? Not sure if its relevant
    partition:,
    memory_per_cpu:,
    priority:,
    qos:, # Quality of Service. can be used to efficiently allocate and manage
            computing resources. The current parameters are:
            [cryo-kornberg, cryoem-kornberg, expedite, limitage, normal, offline,
            part_spearx, preemptable]
    shared:, #if used, resources of the job can be shared with other jobs within
            the same computer node. can achieve higher resource utilization.
            Not sure if its relevant to share resourced between all the accounts
            or within accounts to properly fragmentate the reources at USDF
    start_time:,
    submit_time:,
    user_name:

Metrics to compute per user_name -| partition -| account. For general view
- Amount of Jobs in different state
- Stacked version of running CPUs in function of priority:
    - Usefull to check within USDF if a job is not running for an user due that
    is using low priority. Also check if high priority jobs are starving the pool
- Amount of used nodes.
- Amount of unassigned CPUs/Mem
- Amount of running CPUs / Amount of mem
- Long jobs per user (Jobs taking more than X time running).
- Number of task asked per user_name

Metrics to compute per user_name -| partition -| account. For dev view:
- Check How good a node was partitioned. Core per node vs unassigned cores.
- Performance Utilization per user_name. cpus / sum cores per node if:
    - shared =  True
    - qos = [inner parameters]
- General load and utilization of resources in USDF.

idb_tags = ["account", "job_state", "partition", "priority", "qos",
"user_name","facility","repo","long_pending","long_running","multi_partition",
"multi_host", "node","cpus","memory_per_cpu"]
idb_fields = ["job_id","cpus","task","memory","cores","priority"]

"""

def get_working_nodes(string_node):
    prefix = string_node.split("[")[0]
    ranges = string_node.split("[")[1].rstrip("]").split(",")
    nodes = []
    for node in ranges:
        if "-" in node:
            start, end = node.split("-")
            nodes.extend([f"{prefix}{i:03}" for i in range(int(start), int(end) + 1)])
        else:
            nodes.append(f"{prefix}{int(node):03}")
    return nodes

def influxDBLineProtocol(measurement_name,input_dict):
    """
    Function to return the proper format for influxDB line protocol according to:
    https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol.
    Input dict is as follows.
    {
        tagvalue1:val1, fieldvalue1:val1, ..., othervalue:valuex ...
        idb_tags:[tagvalue1, tagvalue2, ...],
        idb_field:[fieldvalue1, fieldvalue2, ...]
    }
    Empty keys values from input dictionary will be treated as None.
    If the input_dict does not contains pair of keys and values in the list of
    tags, the tag will have a value of "(null)"
    If the input_dict does not contains pair of keys and values in the list of
    fields, the field will have a value of 0
    """

    input_dict = {key: '_' if '.' in value else (None if value == '' else value) for key, value in input_dict.items()}
    formatted_tags = [f"{key}={value}" if value is not None else f"{key}=\\\"\\\"" for key, value in input_dict.items() if key in input_dict['idb_tags']]
    formatted_fields = [f"{key}={value}u" if value is not None else f"{key}=0u" for key, value in input_dict.items() if key in input_dict['idb_fields']]

    formatted_output =','.join([measurement_name] + [','.join(formatted_tags)])
    formatted_output = formatted_output+' '+','.join(formatted_fields)+' '+str(input_dict['timestamp'])

    return formatted_output

def main():
    try:
        parser = argparse.ArgumentParser(description='Describie squeue in json format')
        parser.add_argument("-tRunning", help="Amount of minutes running", type=int, default=1440)
        parser.add_argument("-tPending", help="Amount of minutes pending", type=int, default=30)
        args = parser.parse_args()
        timestamp=int(time.time()*1e9)

        # Execute squeue
        squeue = 'squeue --json'
        out_commandline = subprocess.Popen(squeue, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Read the standard output and decode it as JSON
        stdout, stderr = out_commandline.communicate()
        if stderr:
            print("Error:", stderr.decode('utf-8'))
            exit(1)
        if stdout is None:
            print("No standard output produced by the subprocess.")
            exit(1)
        # Parse the JSON data into a Python dictionary
        json_squeue = json.loads(stdout.decode('utf-8'))

        attributes2check = ["account", "accrue_time", "contiguous","num_task",
                    "job_id", "job_resources", "job_state",
                    "cpus", "node_count", "tasks", "partition", "memory_per_cpu",
                    "priority", "qos", "shared", "start_time", "submit_time",
                    "user_name","memory_per_node","state_reason","name"]
        idb_tags = ["account", "job_state", "partition", "priority", "qos",
                    "user_name","facility","repo","long_pending","long_running",
                    "multi_partition","multi_host", "node","state_reason","name"]
        idb_fields = ["job_id","cpus","task","memory","cores","memory_per_cpu","memory_per_node"]

        for jobs in json_squeue['jobs']:
            sub_dict = {key: jobs[key] for key in attributes2check if key in jobs}
            sub_dict['timestamp'] =  timestamp
            sub_dict['idb_tags'] = idb_tags
            sub_dict['idb_fields'] = idb_fields
            #Add new concept to monitor resources by experiment and by sesion.
            if ":" in sub_dict['account']:
                facility_repo=sub_dict['account'].split(":")
                facility=facility_repo[1]
                repo=facility_repo[0]
                sub_dict['facility']=facility
                sub_dict['repo']=repo
            else:
                sub_dict['facility'] = None
                sub_dict['repo'] = None
            if sub_dict['job_state'] == "PENDING" and ((timestamp - sub_dict['submit_time']) > args.tPending*60):
                sub_dict['time_pending'] = timestamp - sub_dict['submit_time']
                sub_dict['long_pending'] = True
            else:
                sub_dict['long_pending'] = False
            if sub_dict['start_time'] != 0 and ((timestamp - sub_dict['submit_time']) > args.tRunning*60):
                sub_dict['time_running'] = timestamp - sub_dict['submit_time']
                sub_dict['long_running'] = True
            else:
                sub_dict['long_running'] = False
            #Check if job is with  multiple partitions: If a job is running in multiple partitions this should
            #handle the distribution of the resources. TO-CHECK
            if "," in sub_dict['partition']:
                sub_dict['multi_partition'] = True
                partitions = sub_dict['partition'].split(",")
                for partition in partitions:
                    sub_dict.update({'partition':partition})
                    if len(sub_dict['job_resources'])!=0:
                        #Check for multiple nodes and split information
                        if sub_dict.get('job_resources',{}).get('allocated_hosts') > 1:
                            sub_dict['multi_host'] = True
                            nodes=get_working_nodes(sub_dict['job_resources']['nodes'])
                        else:
                            sub_dict['multi_host'] = False
                            nodes=[sub_dict['job_resources']['nodes']]
                        for nodenumber in sub_dict['job_resources']['allocated_nodes'].keys():
                            sub_dict['node']=nodes[int(nodenumber)]
                            sub_dict['memory']=sub_dict['job_resources']['allocated_nodes'][nodenumber]['memory']
                            sub_dict['cores']=sub_dict['job_resources']['allocated_nodes'][nodenumber]['cpus']
                            dict_influxdb=copy.deepcopy(sub_dict)
                            del dict_influxdb['job_resources']
                            output_formatted=influxDBLineProtocol("squeue_json",dict_influxdb)
                            print(output_formatted)
                    else:
                        dict_influxdb=copy.deepcopy(sub_dict)
                        del dict_influxdb['job_resources']
                        output_formatted=influxDBLineProtocol("squeue_json",dict_influxdb)
                        print(output_formatted)

            else:
                sub_dict['multi_partition'] = False
                if len(sub_dict['job_resources'])!=0:
                        if sub_dict.get('job_resources',{}).get('allocated_hosts') > 1:
                            sub_dict['multi_host'] = True
                            nodes=get_working_nodes(sub_dict['job_resources']['nodes'])
                        else:
                            sub_dict['multi_host'] = False
                            nodes=[sub_dict['job_resources']['nodes']]
                        for nodenumber in sub_dict['job_resources']['allocated_nodes'].keys():
                            sub_dict['node']=nodes[int(nodenumber)]
                            sub_dict['memory']=sub_dict['job_resources']['allocated_nodes'][nodenumber]['memory']
                            sub_dict['cores']=sub_dict['job_resources']['allocated_nodes'][nodenumber]['cpus']
                            dict_influxdb=copy.deepcopy(sub_dict)
                            del dict_influxdb['job_resources']
                            output_formatted=influxDBLineProtocol("squeue_json",dict_influxdb)
                            print(output_formatted)
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    sys.exit(main())
