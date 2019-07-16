"""
Generates plots for results from NUMA tests
"""


import os
import re
from datetime import datetime
from datetime import timedelta
import time
import random
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
from collections.abc import Iterable


# Regex patterns. https://regex101.com/
# First line of every SAR file to get date
first_line_regex = r'^Linux.+\s+([0-9]+[/-][0-9]+[/-][0-9]+)\s+'
numa_stat_result_regexp = r'^\[([0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+)\]\s+([^\s]+)\s+([0-9]+)\s+([0-9]+)$'
cpu_any_core_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+([a-z0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)$'
numa_ctl_mem_regexp=r'^\[([0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+)\]\s+node\s+([0-9]+)\s+free:\s+([0-9]+)\s+MB$'
    
exp_min_time = datetime.strptime('2100-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
exp_max_time = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')


# Return list of timestamps as list of seconds since the exp min time
def ts_to_seconds(timestamps):
    if isinstance(timestamps, Iterable):
        return [(key - exp_min_time).total_seconds() for key in timestamps]
    if isinstance(timestamps, datetime):
        return (timestamps - exp_min_time).total_seconds()
    return None


def try_parse_int(str):
    try:
        return int(str)
    except ValueError:
        return None


# Parses date from the first line in SAR output file. SAR outputs different formats at different times for some
# reason. Returns date string in a uniform format, no matter which format SAR outputs in.
def parse_date_from_sar_file(first_line_in_file):
    matches = re.match(first_line_regex, first_line_in_file)
    date_string_match = matches.group(1)
    # e.g., 11/08/2018 or 2018-11-08
    try:
        date_string = datetime.strptime(date_string_match, '%m/%d/%Y').strftime('%m/%d/%Y')
    except ValueError:
        # Try parsing other format
        date_string = datetime.strptime(date_string_match, '%Y-%m-%d').strftime('%m/%d/%Y')
    return date_string


def parse_get_numa_mem_alloc(results_folder, axes, hide_xlabel=False):
    node0_local = defaultdict(float)
    node0_remote = defaultdict(float)
    node1_local = defaultdict(float)
    node1_remote = defaultdict(float)
    global exp_max_time
    global exp_min_time

    mem_bw_file_path = os.path.join(results_folder, "mem_alloc")
    with open(mem_bw_file_path, "r") as lines:

        node0_local_previous = None
        node0_remote_previous = None
        node1_local_previous = None
        node1_remote_previous = None
        for line in lines:        
            matches = re.match(numa_stat_result_regexp, line)
            if matches:
                timestamp = datetime.strptime(matches.group(1), '%Y-%m-%d %H:%M:%S')
                if timestamp < exp_min_time:    exp_min_time = timestamp
                if timestamp > exp_max_time:    exp_max_time = timestamp

                label = matches.group(2)
                node0_accesses = int(matches.group(3))
                node1_accesses = int(matches.group(4))
                if label == "local_node":
                    if node0_local_previous:    node0_local[timestamp] = (node0_accesses - node0_local_previous)
                    if node1_local_previous:    node1_local[timestamp] = (node1_accesses - node1_local_previous)
                    node0_local_previous = node0_accesses
                    node1_local_previous = node1_accesses
                elif label == "other_node":
                    if node0_remote_previous:    node0_remote[timestamp] = (node0_accesses - node0_remote_previous)
                    if node1_remote_previous:    node1_remote[timestamp] = (node1_accesses - node1_remote_previous)
                    node0_remote_previous = node0_accesses
                    node1_remote_previous = node1_accesses


    axes.set_title("Memory Allocator Hits")
    if not hide_xlabel: axes.set_xlabel("Time (Secs)")
    axes.get_xaxis().set_visible(not hide_xlabel)
    axes.set_ylabel("Hits")
    axes.set_xlim(ts_to_seconds(exp_min_time), ts_to_seconds(exp_max_time))
    axes.plot(ts_to_seconds(node0_local.keys()), node0_local.values(), label="Mem Node 0: Local", linestyle='solid')
    axes.plot(ts_to_seconds(node1_local.keys()), node1_local.values(), label="Mem Node 1: Local", linestyle='solid')
    axes.plot(ts_to_seconds(node0_remote.keys()), node0_remote.values(), label="Mem Node 0: Remote", linestyle='dashed')
    axes.plot(ts_to_seconds(node1_remote.keys()), node1_remote.values(), label="Mem Node 1: Remote", linestyle='dashed')
    axes.legend( prop={'size': 8})


def parse_get_numa_mem_access(results_folder, axes, hide_xlabel=False):
    node0_local_readings = defaultdict(float)
    node0_remote_readings = defaultdict(float)
    node1_local_readings = defaultdict(float)
    node1_remote_readings = defaultdict(float)
    global exp_max_time
    global exp_min_time

    mem_bw_file_path = os.path.join(results_folder, "memaccess.csv")
    with open(mem_bw_file_path, "r") as lines:

        node0_local = 0
        node1_local = 0
        node0_remote = 0
        node1_remote = 0 
        previous_timestamp = None
        for line in lines:
            cols = [x.strip() for x in line.split(",") if not x.isspace()]
            if cols.__len__() == 7 and cols[1] == "*":
                # total accesses
                pass

            if cols.__len__() == 7 and try_parse_int(cols[1]):
                timestamp = datetime.strptime(cols[0], '%Y-%m-%d %H:%M:%S')
                if timestamp < exp_min_time:    exp_min_time = timestamp
                if timestamp > exp_max_time:    exp_max_time = timestamp

                cpu_id = try_parse_int(cols[1])
                node_id = cpu_id % 2
                local_accesses = float(cols[5])/1000000
                remote_accesses = float(cols[6])/1000000

                # If reading moves on to the next second, save details of previous second
                # NOTE: Does not record values for last timestamp
                if previous_timestamp and previous_timestamp != timestamp:
                    node0_local_readings[previous_timestamp] = node0_local
                    node1_local_readings[previous_timestamp] = node1_local
                    node0_remote_readings[previous_timestamp] = node0_remote
                    node1_remote_readings[previous_timestamp] = node1_remote
                    node0_local = 0
                    node1_local = 0
                    node0_remote = 0
                    node1_remote = 0

                if not node_id:
                    node0_local += local_accesses
                    node0_remote += remote_accesses
                else:
                    node1_local += local_accesses
                    node1_remote += remote_accesses

                previous_timestamp = timestamp

    axes.set_title("Memory Acesses")
    if not hide_xlabel: axes.set_xlabel("Time (Secs)")
    axes.get_xaxis().set_visible(not hide_xlabel)
    axes.set_ylabel("Acesses (Millions)")
    axes.set_xlim(ts_to_seconds(exp_min_time), ts_to_seconds(exp_max_time))
    axes.plot(ts_to_seconds(node0_local_readings.keys()), node0_local_readings.values(), label="CPU Node 0: Local", linestyle='solid')
    axes.plot(ts_to_seconds(node1_local_readings.keys()), node1_local_readings.values(), label="CPU Node 1: Local", linestyle='solid')
    axes.plot(ts_to_seconds(node0_remote_readings.keys()), node0_remote_readings.values(), label="CPU Node 0: Remote", linestyle='dashed')
    axes.plot(ts_to_seconds(node1_remote_readings.keys()), node1_remote_readings.values(), label="CPU Node 1: Remote", linestyle='dashed')
    axes.legend( prop={'size': 8})


def parse_get_numa_ipc(results_folder, axes, hide_xlabel=False):
    ipc_readings = defaultdict(float)
    global exp_max_time
    global exp_min_time

    mem_bw_file_path = os.path.join(results_folder, "memaccess.csv")
    with open(mem_bw_file_path, "r") as lines:
        for line in lines:
            cols = [x.strip() for x in line.split(",") if not x.isspace()]
            if cols.__len__() == 7 and cols[1] == "*":
                timestamp = datetime.strptime(cols[0], '%Y-%m-%d %H:%M:%S')
                if timestamp < exp_min_time:    exp_min_time = timestamp
                if timestamp > exp_max_time:    exp_max_time = timestamp
                ipc = float(cols[2])
                ipc_readings[timestamp] = ipc

    axes.set_title("IPC")
    if not hide_xlabel: axes.set_xlabel("Time (Secs)")
    axes.get_xaxis().set_visible(not hide_xlabel)
    axes.set_ylabel("IPC")
    axes.set_xlim(ts_to_seconds(exp_min_time), ts_to_seconds(exp_max_time))
    axes.plot(ts_to_seconds(ipc_readings.keys()), ipc_readings.values())
    # axes.legend( prop={'size': 8})


def parse_get_numa_mem_usage(results_folder, axes, hide_xlabel=False):
    node0_used = defaultdict(float)
    node1_used = defaultdict(float)
    global exp_max_time
    global exp_min_time

    mem_bw_file_path = os.path.join(results_folder, "mem_usage")
    with open(mem_bw_file_path, "r") as lines:
        for line in lines:        

            matches = re.match(numa_ctl_mem_regexp, line)
            if matches:
                timestamp = datetime.strptime(matches.group(1), '%Y-%m-%d %H:%M:%S')
                if timestamp < exp_min_time:    exp_min_time = timestamp
                if timestamp > exp_max_time:    exp_max_time = timestamp

                node_id = int(matches.group(2))
                mem_free_mb = float(matches.group(3))
                if node_id % 2:
                    node1_used[timestamp] = (129015 - mem_free_mb)/1024
                else:
                    node0_used[timestamp] = (128492 - mem_free_mb)/1024

    axes.set_title("Memory Usage")
    if not hide_xlabel: axes.set_xlabel("Time (Secs)")
    axes.get_xaxis().set_visible(not hide_xlabel)
    axes.set_ylabel("GB")
    axes.set_xlim(ts_to_seconds(exp_min_time), ts_to_seconds(exp_max_time))
    axes.plot(ts_to_seconds(node0_used.keys()), node0_used.values(), label="Node 0", linestyle='solid')
    axes.plot(ts_to_seconds(node1_used.keys()), node1_used.values(), label="Node 1", linestyle='solid')
    axes.legend(prop={'size': 8})


def parse_get_numa_cpu_usage(results_folder, axes, hide_xlabel=False):
    total_cpu_readings = defaultdict(float)
    node0_cpu_readings = defaultdict(float)
    node1_cpu_readings = defaultdict(float)
    global exp_max_time
    global exp_min_time

    cpu_file_path = os.path.join(results_folder, "cpu_usage")
    with open(cpu_file_path, "r") as lines:
        node0_cpu = 0
        node1_cpu = 0
        first_line = True
        date_part = None
        previous_timestamp = None
        for line in lines:
            if first_line:
                date_string = parse_date_from_sar_file(first_line_in_file=line)
                date_part = datetime.strptime(date_string, '%m/%d/%Y')
                first_line = False

            matches = re.match(cpu_any_core_regex, line)
            if matches:
                # Extract timesamp. NOTE: Does not deal with experiment running into the next day
                time_string = matches.group(1)
                time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                if timestamp < exp_min_time:    exp_min_time = timestamp
                if timestamp > exp_max_time:    exp_max_time = timestamp

                # If reading moves on to the next second, save details of previous second
                # NOTE: Does not record values for last timestamp
                if previous_timestamp and previous_timestamp != timestamp:
                    node0_cpu_readings[previous_timestamp] = node0_cpu/80
                    node1_cpu_readings[previous_timestamp] = node1_cpu/80
                    node0_cpu = 0
                    node1_cpu = 0
                
                cpu_label = matches.group(2)
                cpu_user_usage = float(matches.group(3))
                cpu_system_usage = float(matches.group(5))
                cpu_total_usage = cpu_user_usage + cpu_system_usage

                if cpu_label == "all":
                    total_cpu_readings[timestamp] = cpu_total_usage
                else:
                    cpu_id = int(cpu_label)
                    numa_node = cpu_id % 2
                    if not numa_node:
                        node0_cpu += cpu_total_usage
                    else:
                        node1_cpu += cpu_total_usage

                previous_timestamp = timestamp


    axes.set_title("CPU Usage")
    if not hide_xlabel: axes.set_xlabel("Time (Secs)")
    axes.get_xaxis().set_visible(not hide_xlabel)
    axes.set_xlim(ts_to_seconds(exp_min_time), ts_to_seconds(exp_max_time))
    axes.set_ylabel("%")
    # axes.plot(total_cpu_readings.keys(), total_cpu_readings.values(), label="Total")
    axes.plot(ts_to_seconds(node0_cpu_readings.keys()), node0_cpu_readings.values(), label="Node 0")
    axes.plot(ts_to_seconds(node1_cpu_readings.keys()), node1_cpu_readings.values(), label="Node 1")
    axes.legend(prop={'size': 8})


def main():
    
    experiments = [
        # ("results\\no_numactl",                 'No NUMACTL'),
        # ("results\\all_node0",                  'NUMACTL: numactl --cpunodebind=0 --membind=0'),
        # ("results\\all_node1",                  'NUMACTL: numactl --cpunodebind=1 --membind=1'),
        # ("results\\cpu_node0_mem_mode1",        'NUMACTL: numactl --cpunodebind=0 --membind=1'),
        # ("results\\cpu_node0_interleave0",      'NUMACTL: numactl --cpunodebind=0 --interleave=0'),
        # ("results\\cpu_node0_interleave01",     'NUMACTL: numactl --cpunodebind=0 --interleave=0,1'),
        # ("results\\cpu_node01_interleave01",    'NUMACTL: numactl --cpunodebind=0,1 --interleave=0,1'),
        # ("results\\SortTestRun2",               "300GB Sort, NUMA enabled, 2 Containers, b09-40"),
        # ("results\\SortTestRun3",               "300GB Sort, NUMA disabled, 1 Container, b09-34"),
        # ("results\\SortTestRun4",               "300GB Sort, NUMA disabled, 1 Container, b09-34"),
        # ("results\\SortTestRun5",               "300gb run; NUMA enabled in YARN, 2 Containers, b09-34"),
        # ("results\\SortTestRun6",               "300gb run; NUMA enabled in YARN and JVM, 2 Containers, b09-34"),
        # ("results\\SortTestRun7",               "300gb run; NUMA actually working in YARN, B09-34"),
        # ("results\\SortTestRun8",               "300gb run; NUMA actually working in YARN, B09-34"),
    ]

    experiments = ["SortTestRun9", "SortTestRun10", "SortTestRun11", "SortTestRun12"]

    global exp_max_time
    global exp_min_time
    for idx, exp_folder in enumerate(experiments):
        results_folder = os.path.join("results", exp_folder)
        exp_min_time = datetime.strptime('2100-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        exp_max_time = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

        # Get description if exists
        desc_file = os.path.join(results_folder, "desc")
        if os.path.exists(desc_file):
            title = open(desc_file, 'r').read()

        fig = plt.figure()
        fig.set_size_inches(w=10,h=5)
        fig.suptitle(title)
        grid = plt.GridSpec(4, 4, wspace=0.4, hspace=0.3)
        
        parse_get_numa_cpu_usage(results_folder, axes=plt.subplot(grid[0:2, 0:2]), hide_xlabel=True)
        # parse_get_numa_mem_alloc(results_folder, axes=plt.subplot(grid[2:4, 0:2]))
        parse_get_numa_ipc(results_folder, axes=plt.subplot(grid[2:4, 0:2]))
        parse_get_numa_mem_usage(results_folder, axes=plt.subplot(grid[0:2, 2:4]), hide_xlabel=True)
        parse_get_numa_mem_access(results_folder, axes=plt.subplot(grid[2:4, 2:4]))


        output_plot_file_name = "numa_plot_{0}_v2.png".format(idx)
        output_full_path = os.path.join(results_folder, output_plot_file_name)
        plt.savefig(output_full_path)
        # plt.show()
        plt.close()


if __name__ == '__main__':
    main()