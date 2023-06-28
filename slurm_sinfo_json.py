import subprocess
import json
import copy
import time
import sys
import traceback
import datetime

"""
    All the parameters from sinfo --json seems to be easy to understand. The
    parameter weight can be used to diagnose a working node without force
    that particular node to drain or do a hard reboot. So far weight has the
    same value for all the working nodes and this paratemer represent a relative
    value assigned to each node in the cluster that reflects the nodes capacity
    or performance. Slurm use this information to distribute the jobs across the
    cluster. with higher-weighted nodes receiving more queue of the total workload
"""

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
    If the input_dict does not contains pair of keys and values in the list of
    tags, the tag will have a value of "(null)"
    If the input_dict does not contains pair of keys and values in the list of
    fields, the field will have a value of 0
    """

    formatted_tags = [f"{key}={value}" if value is not None else f"{key}=(null)" for key, value in input_dict.items() if key in input_dict['idb_tags']]
    formatted_fields = [f"{key}=\"{(value if value is not None and value != '' else '(null)')}\"" if isinstance(value, str) else f"{key}={value}i" if value is not None else f"{key}=0" for key, value in input_dict.items() if key in input_dict['idb_fields']]

    formatted_output =','.join([measurement_name] + [','.join(formatted_tags)])
    formatted_output = formatted_output+' '+' '.join(formatted_fields)+' '+str(input_dict['timestamp'])

    return formatted_output

def main():
    try:
        timestamp=int(time.time()*1e9)
        # Execute sinfo
        sinfo = 'sinfo --json'
        out_commandline = subprocess.Popen(sinfo, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Read the standard output and decode it as JSON
        stdout, stderr = out_commandline.communicate()
        if stderr:
            print("Error:", stderr.decode('utf-8'))
            exit(1)
        if stdout is None:
            print("No standard output produced by the subprocess.")
            exit(1)
        # Parse the JSON data into a Python dictionary
        json_sinfo = json.loads(stdout.decode('utf-8'))


        attributes2check = ["hostname","state","reason","cpus",
        "alloc_cpus","idle_cpus","real_memory","alloc_memory","free_memory", "weight",
        "boot_time", "slurmd_start_time"]

        idb_tags = ["hostname", "state", "weight"]
        idb_fields = ["reason", "alloc_cpus", "idle_cpus", "alloc_memory",
        "free_memory","boot_time_s","slurmd_start_time_s"]

        for node in json_sinfo['nodes']:
            sub_dict = {key: node[key] for key in attributes2check if key in node}
            if sub_dict['boot_time'] or sub_dict['slurmd_start_time']:
                date_bootTime = datetime.datetime.fromtimestamp(sub_dict['boot_time'])
                date_slurmd_s_time = datetime.datetime.fromtimestamp(sub_dict['slurmd_start_time'])
                sub_dict['boot_time_s'] = str(date_bootTime.strftime('%Y-%m-%d %H:%M:%S'))
                sub_dict['slurmd_start_time_s'] = str(date_slurmd_s_time.strftime('%Y-%m-%d %H:%M:%S'))
            sub_dict['timestamp'] =  timestamp
            sub_dict['idb_tags'] = idb_tags
            sub_dict['idb_fields'] = idb_fields
            output_formatted=influxDBLineProtocol("sinfo_json",sub_dict)
            print(output_formatted)


    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    sys.exit(main())
