"""
Code for all the miscellaneous one-time plots
"""


import os
import re
from datetime import datetime
from datetime import timedelta
import time
import random
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
import plot_one_experiment
import socket
import run_experiments


# A simplified way to get IP Addr to Node name mapping
def ip_to_name(node_name):
    ip_to_node_dict = {}
    for node, addr in run_experiments.fat_tree_ip_mac_map.items():
        ip_to_node_dict[addr[1]] = node
    return ip_to_node_dict[node_name] if node_name in ip_to_node_dict.keys() else node_name


# A simplified way to get port to svc name mapping
def port_to_svc(port):
    # General: 500** - hdfs, 80** - yarn
    port_to_svv_map = {
        10000 : "hdfs_namenode",
        50010 : "hdfs_datanode_data",
        50075 : "hdfs_datanode_http",
        50020 : "hdfs_datanode_ipc",

    }
    return port_to_svv_map[port] if port in port_to_svv_map.keys() else str(port)


# Generte plots for memory bandwidth readings taken from a node during a spark sort run
def plot_mem_bandwidth():
    experiment_id = "Exp-2019-05-09-16-24-14" # "Exp-2019-05-09-16-29-38" # "Exp-2019-02-19-10-02-33"
    experiment_dir_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
    mem_bw_file_path = os.path.join(experiment_dir_path, "mb_output")
    values = Counter()
    i = 0
    line_regex = r'^\s+0-39\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+([0-9]+[\.]?[0-9]+)\s+[^\s]+$'
    with open(mem_bw_file_path, "r") as lines:
        for line in lines:
            matches = re.match(line_regex, line)
            if matches:
                value = float(matches.group(1))
                values[i] = value
                i += 1
    
    fig, ax = plt.subplots(1, 1)
    fig.suptitle("Local Mem Bandwidth ")
    ax.set_xlabel("Seconds")
    ax.set_ylabel("MB/s")
    ax.plot(values.keys(), values.values())
    plt.show()

    # fig, ax = plt.subplots(1, 1)
    # fig.suptitle("Mem access rate for Spark Sort 100GB")
    # ax.set_xlabel("Mem access rate MB/s")
    # ax.set_ylabel("CDF")
    # readings = list(values.values())
    # readings = [r for r in readings if r > 10]
    # x, y = plot_one_experiment.gen_cdf_curve(readings, 1000)
    # ax.plot(x, y)
    # plt.show()


# Plots to analyze network bandwidth breakdowm per service network packet trace collected from a node 
# durign a spark sort run
def plot_network_pkt_trace():
    experiment_id =  "Exp-2019-04-16-15-40-02" 
    source_node = "b09-38"
    # Others (b09-32): "Exp-2019-04-11-15-02-16" "Exp-2019-04-11-15-51-56" "Exp-2019-04-11-16-13-40" "Exp-2019-04-11-18-21-58"
    # Others (b09-38): "Exp-2019-04-16-15-40-02"
    experiment_dir_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
    net_pkt_trace_path = os.path.join(experiment_dir_path, "net_usage_breakdown")
    line_regex = r'^([0-9]+.[0-9]+)\s+([0-9]+.[0-9]+.[0-9]+.[0-9]+)\s+([0-9]+)\s+([0-9]+.[0-9]+.[0-9]+.[0-9]+)\s+([0-9]+)\s+([0-9]+)$'
    # line_regex = r'^([0-9]+.[0-9]+)\s+(.+)\s+([0-9]+)$'

    thrpt_out_by_label = {}
    thrpt_in_by_label = {}
    total_thrpt_in = Counter()
    total_thrpt_out = Counter()
    with open(net_pkt_trace_path, "r") as lines:
        for line in lines:
            matches = re.match(line_regex, line)
            ts = datetime.fromtimestamp(int(float(matches.group(1))))
            # protocol = matches.group(2)
            src_ip = matches.group(2)
            src_name = ip_to_name(src_ip)
            src_port = int(matches.group(3))
            dst_ip = matches.group(4)
            dst_name = ip_to_name(dst_ip)
            dst_port = int(matches.group(5))
            frame_size = float(matches.group(3))

            if src_name == source_node:
                label = dst_name + ":" + port_to_svc(dst_port)
                if label not in thrpt_out_by_label.keys():
                    thrpt_out_by_label[label] = Counter()
                thrpt_out_by_label[label][ts] += frame_size
                total_thrpt_out[ts] += frame_size           
            
            if dst_name == source_node:
                label = src_name + ":" + port_to_svc(src_port)
                if label not in thrpt_in_by_label.keys():
                    thrpt_in_by_label[label] = Counter()
                thrpt_in_by_label[label][ts] += frame_size
                total_thrpt_in[ts] += frame_size
            
            if src_name == source_node and dst_name == source_node:
                raise Exception("Boom!! Not what I expect")

    for type_ in ["outbound", "inbound"]:
        total_thrpt = total_thrpt_in if type_ == "inbound" else total_thrpt_out
        thrpt_by_label = thrpt_in_by_label if type_ == "inbound" else thrpt_out_by_label

        # Plot total throughput
        fig, ax = plt.subplots(1, 1)
        fig.set_size_inches(w=10,h=10)
        fig.suptitle("Net {0} xput on b09-32: tshark - with capture drops".format(type_))
        ax.set_xlabel("Time secs")
        ax.set_ylabel("Network throughput Mbps")
        ax.plot(total_thrpt.keys(), [v*8.0/1000000 for v in total_thrpt.values()])
        output_full_path = os.path.join(experiment_dir_path, "net_{0}_tshark_total_thrpt.png".format(type_))
        plt.savefig(output_full_path)
        # plt.show()
        plt.close()

        # Plot throughput by label
        fig, ax = plt.subplots(1, 1)
        fig.set_size_inches(w=10,h=10)
        fig.suptitle("Net {0} xput by <dst host, port> on b09-32: tshark - with capture drops".format(type_))
        ax.set_xlabel("Time secs")
        ax.set_ylabel("Network throughput Mbps")
        top_10_flows_by_total_usage = sorted(thrpt_by_label.keys(), key=lambda k: sum(thrpt_by_label[k].values()), reverse=True)[:30]
        for label in top_10_flows_by_total_usage:
            ax.plot(thrpt_by_label[label].keys(), [v*8.0/1000000 for v in thrpt_by_label[label].values()], label=label)
        plt.legend()
        output_full_path = os.path.join(experiment_dir_path, "net_{0}_tshark_thrpt_30.png".format(type_))
        plt.savefig(output_full_path)
        # plt.show()
        plt.close()


def plot_numa_mem_access():
    experiments = ["Exp-2019-07-01-17-27-37", "Exp-2019-07-01-17-56-53", "Exp-2019-06-26-23-21-14" , "Exp-2019-06-19-14-37-41", "Exp-2019-06-19-15-06-01"]
    for experiment_id in experiments:
        experiment_dir_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
        mem_bw_file_path = os.path.join(experiment_dir_path, "memaccess.csv")
        local_accesses = Counter()
        remote_accesses = Counter()
        i = 0
        with open(mem_bw_file_path, "r") as lines:
            for line in lines:
                cols = line.split(",")
                if cols and cols[0] == "*":
                    # print(cols)
                    local_accesses[i] = int(cols[4])/1000000.0
                    remote_accesses[i] = int(cols[5])/1000000.0
                    i += 1
        
        fig, ax = plt.subplots(1, 1)
        # fig.set_size_inches(w=5,h=5)
        fig.suptitle("Exp ID: {0} - NUMA Memory Accesses (Corrected!)".format(experiment_id))
        ax.set_xlabel("Time (Secs)")
        ax.set_ylabel("Acceses (in millions)")
        ax.set_ylim(0, 1000)
        ax.plot(local_accesses.keys(), local_accesses.values(), label="Local")
        ax.plot(remote_accesses.keys(), remote_accesses.values(), label="Remote")
        ax.plot(local_accesses.keys(), [x+y for x,y in zip(local_accesses.values(), remote_accesses.values())], label="Total")

        plt.legend()
        output_full_path = os.path.join(experiment_dir_path, "numa_mem_access_corrected_1.png")
        plt.savefig(output_full_path)
        # plt.show()


# Collects all results from SAR and Powermeter, parses for required info and merges results onto single timeline.
cpu_any_core_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+([a-z0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)$'

def plot_numa_cpu_usage():
    experiment_id = "Exp-2019-07-02-18-05-51"
    experiment_folder_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
    
    total_cpu_readings = defaultdict(float)
    node0_cpu_readings = defaultdict(float)
    node1_cpu_readings = defaultdict(float)
    node0_cpu = 0
    node1_cpu = 0
    cpu_file_path = os.path.join(experiment_folder_path, "b09-40", "cpu.sar")
    with open(cpu_file_path, "r") as lines:
        first_line = True
        date_part = None
        previous_timestamp = None
        for line in lines:
            if first_line:
                date_string = plot_one_experiment.parse_date_from_sar_file(first_line_in_file=line)
                date_part = datetime.strptime(date_string, '%m/%d/%Y')
                first_line = False

            matches = re.match(cpu_any_core_regex, line)
            if matches:
                # Extract timesamp. NOTE: Does not deal with experiment running into the next day
                time_string = matches.group(1)
                time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)

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

    for key,val in node0_cpu_readings.items():
        print(key, val, node1_cpu_readings[key])

    fig, ax = plt.subplots(1, 1)
    # fig.set_size_inches(w=15,h=7)
    fig.suptitle("CPU Usage by NUMA node")
    ax.set_xlabel("Time")
    ax.set_ylabel("CPU %")

    # ax.plot(total_cpu_readings.keys(), total_cpu_readings.values(), label="Total")
    ax.plot(node0_cpu_readings.keys(), node0_cpu_readings.values(), label="Node 0")
    ax.plot(node1_cpu_readings.keys(), node1_cpu_readings.values(), label="Node 1")

    # Save the file, should be done before show()
    plt.legend()
    output_plot_file_name = "plot_{0}.png".format("cpu_usage_by_numa_node")
    output_full_path = os.path.join(experiment_folder_path, output_plot_file_name)
    plt.savefig(output_full_path)
    plt.show()
    plt.close()


if __name__ == '__main__':
    # plot_mem_bandwidth()
    # plot_network_pkt_trace()
    plot_numa_mem_access()
    # plot_numa_cpu_usage()