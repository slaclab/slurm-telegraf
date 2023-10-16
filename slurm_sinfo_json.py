import subprocess
import json
import copy
import time
import sys
import traceback
import datetime
import re

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
    Empty keys values from input dictionary will be treated as None.
    If the input_dict does not contains pair of keys and values in the list of
    tags, the tag will have a value of "(null)"
    If the input_dict does not contains pair of keys and values in the list of
    fields, the field will have a value of 0
    """

    input_dict = {key: (None if value == '' else (value.replace(' ', '_') if isinstance(value, str) else value)) for key, value in input_dict.items()}
    formatted_tags = [f"{key}={value}" if value is not None else f"{key}=\\\"\\\"" for key, value in input_dict.items() if key in input_dict['idb_tags']]
    formatted_fields = [
    f'{key}="{value}"' if isinstance(value, str)
    else f'{key}={value}i' if isinstance(value, int)
    else f'{key}={value}' if isinstance(value, float)
    else ''
    for key, value in input_dict.items()
    if value is not None and (key in input_dict.get('idb_fields', []))
    ]
    formatted_output =','.join([measurement_name] + [','.join(formatted_tags)])
    formatted_output = formatted_output+' '+','.join(formatted_fields)+' '+str(input_dict['timestamp'])

    return formatted_output

def find_reservation_by_node(data, node):
    for key, value in data.items():
        if node in value['Nodes']:
            return key
    return None

def expand_node_list(node_string):
    nodes = []
    node_groups = re.split(r',(?![^\[]*\])', node_string)

    for node_group in node_groups:
        if '[' in node_group:
            match = re.match(r'^(.*?)\[(.*?)\]$', node_group)
            if match:
                base_name, ranges = match.groups()
                ranges = ranges.split(',')
                for item in ranges:
                    if '-' in item:
                        start, end = item.split('-')
                        nodes.extend([f"{base_name}{str(i).zfill(3)}" for i in range(int(start), int(end) + 1)])
                    else:
                        nodes.append(f"{base_name}{item.zfill(3)}")
        else:
            nodes.append(node_group)
    return nodes

def parse_reservation_info(output):
    reservations = output.split("\n\n")  # Split reservations by double newline
    reservation_dict = {}
    for reservation in reservations:
        if not reservation:  # Skip empty entries
            continue
        attributes = {}
        lines = reservation.split("\n")
        for line in lines:
            parts = line.split()
            for part in parts:
                key, sep, value = part.partition("=")
                if key == "ReservationName":
                    attributes["Name"] = value
                elif key == "Nodes":
                    attributes["Nodes"] = expand_node_list(value)
                elif key == "TRES":
                    cpu_value = value.split(",")[0].split("=")[1]
                    attributes["TRES"] = {"CPU": int(cpu_value)}
                elif key == "Users":
                    attributes["Users"] = value if value != "(null)" else None
                elif key == "Accounts":
                    attributes["Accounts"] = value.split(",") if value != "(null)" else None
        reservation_dict[attributes["Name"]] = attributes

    return reservation_dict

def get_reservation_info():
    try:
        result = subprocess.run(['scontrol', 'show', 'reservation'], stdout=subprocess.PIPE, universal_newlines=True)
        return parse_reservation_info(result.stdout)
    except Exception as e:
        print(f"Error: {e}")
        return {}


def tres_attribute(data):
    # Split the value string into key-value pairs
    if data == "null":
        result={'tres_cpu':0,
                'tres_mem':0,
                'gpu_count':0,
                'gpu_type': None}
    else:
        parts = data.split(',')

        # Create a dictionary to store results
        result = {}

        # Iterate over parts to convert them
        for part in parts:
            key, value = part.split('=')

            # Handle 'cpu' key
            if 'cpu' in key:
                result[f'tres_{key}'] = int(value)
            # Handle 'mem' key and its value with units
            elif 'mem' in key:
                if 'G' in value:  # Gigabytes
                    result[f'tres_{key}'] = float(value[:-1]) * 1e9
                elif 'M' in value:  # Megabytes
                    result[f'tres_{key}'] = float(value[:-1]) * 1e6
            # Handle 'gres/gpu' key and its value
            elif 'gres/gpu' in key and ':' not in key:
                result['gpu_count'] = int(value)
            # Handle 'gres/gpu:type' key and its value
            elif 'gres/gpu:' in key:
                result['gpu_type'] = key.split(':')[-1]

    return result

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
        # Nested Dictionary with the following convention for reserved nodes
        # {ReservationName,{Name:ReservationName,
        #                   Nodes: list[],
        #                   TRES:{'CPU':NumberCPUs},
        #                   Users: list[],
        #                   Accounts : list[]}
        reservations = get_reservation_info()

        attributes2check = ["hostname","state","reason","cpus",
        "alloc_cpus","idle_cpus","real_memory","alloc_memory","free_memory", "weight",
        "boot_time", "slurmd_start_time","cpu_load","state_flags","tres_weighted","tres","tres_used"]

        idb_tags = ["hostname", "state","weight","reason","state_flags","reservation_name","tres_gpu_type"]
        idb_fields = ["alloc_cpus", "idle_cpus", "alloc_memory",
        "free_memory","boot_time_s","slurmd_start_time_s","state_f","reason_f",
        "cpu_load","state_flags_f","tres_weighted","reservation_name_f","tres_cpu","tres_mem",
        "tres_used_cpu","tres_used_mem","tres_gpu_count","tres_used_gpu_count"]

        for node in json_sinfo['nodes']:
            sub_dict = {key: node[key] for key in attributes2check if key in node}
            sub_dict['timestamp'] =  timestamp
            sub_dict['idb_tags'] = idb_tags
            sub_dict['idb_fields'] = idb_fields
            sub_dict['state_f']=sub_dict['state']
            sub_dict['reason_f']=sub_dict['reason']
            if sub_dict['state_flags']:
                sub_dict['state_flags']=sub_dict['state_flags'][0]
                sub_dict['state_flags_f']=sub_dict['state_flags']
            else:
                sub_dict['state_flags'] = None
                sub_dict['state_flags_f'] = None
            if sub_dict['state_flags_f'] in ["RESERVED","MAINTENANCE","DRAIN"]:
                sub_dict['reservation_name']=find_reservation_by_node(reservations,sub_dict['hostname'])
                sub_dict['reservation_name_f']=sub_dict['reservation_name']
            else:
                sub_dict['reservation_name'] = None
                sub_dict['reservation_name_f'] =  None
            #Getting the true values of usable cores/mem and usage by node
            if sub_dict.get("tres") and '=' in sub_dict["tres"]:
                tres_dict = tres_attribute(sub_dict["tres"])
                sub_dict["tres_cpu"] = int(tres_dict.get("tres_cpu",0))
                sub_dict["tres_mem"] = int(tres_dict.get("tres_mem",0))
                sub_dict["tres_gpu_count"] = tres_dict.get("gpu_count",None)
                sub_dict["tres_gpu_type"] = tres_dict.get("gpu_type",None)
            else:
                sub_dict["tres_cpu"] = 0
                sub_dict["tres_mem"] = 0
                sub_dict["tres_gpu_count"] = 0
                sub_dict["tres_gpu_type"] = None
            if sub_dict.get("tres_used") and '=' in sub_dict["tres_used"]:
                tres_dict_used = tres_attribute(sub_dict["tres_used"])
                sub_dict["tres_used_cpu"] = int(tres_dict_used.get("tres_cpu",0))
                sub_dict["tres_used_mem"] = int(tres_dict_used.get("tres_mem",0))
                sub_dict["tres_used_gpu_count"] = tres_dict_used.get("gpu_count", 0)
                sub_dict["tres_used_gpu_type"] = tres_dict_used.get("gpu_type", None)
            else:
                sub_dict["tres_used_cpu"] = 0
                sub_dict["tres_used_mem"] = 0
                sub_dict["tres_used_gpu_count"] = 0
                sub_dict["tres_used_gpu_type"] = None

            output_formatted=influxDBLineProtocol("sinfo_json",sub_dict)
            print(output_formatted)


    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    sys.exit(main())
