"""
Aggregates Power and other metrics across multiple experiments and generates plots
"""

import argparse
import os
import re
import math
import shutil
from datetime import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
import plot_one_experiment
from plot_one_experiment import ExperimentSetup
import numpy as np
from pprint import pprint
import json


class ExperimentMetrics:
    experiment_id = None
    experiment_setup = None
    experiment_start_time = None
    input_size_gb = None
    link_bandwidth_mbps = None
    duration = None
    total_power_all_nodes = None
    per_node_metrics_dict = None
    stages_start_end_times = {}
    precise_total_time = None
    precise_map_time = None
    precise_reduce_time = None

    def __init__(self, experiment_id, experiment_setup, per_node_metrics_dict):
        self.experiment_id = experiment_id
        self.experiment_setup = experiment_setup
        self.experiment_start_time = experiment_setup.experiment_start_time
        self.input_size_gb = experiment_setup.input_size_gb
        self.link_bandwidth_mbps = experiment_setup.link_bandwidth_mbps
        self.duration = (experiment_setup.spark_job_end_time - experiment_setup.spark_job_start_time)
        self.per_node_metrics_dict = per_node_metrics_dict
        self.total_power_all_nodes = sum([n.total_power_consumed for n in per_node_metrics_dict.values() if n.total_power_consumed is not None])
        self.total_net_in_KB_all_nodes = sum([n.total_net_in_kBps for n in per_node_metrics_dict.values() if n.total_net_in_kBps is not None])
        self.total_net_out_KB_all_nodes = sum([n.total_net_out_kBps for n in per_node_metrics_dict.values() if n.total_net_out_kBps is not None])

    def get_plot_friendly_name(self):
        return self.experiment_setup.plot_friendly_name or (self.experiment_setup.scala_class_name + ":" + self.experiment_id)


class ExperimentPerNodeMetrics:
    total_power_consumed = None
    per_stage_power_list = []
    total_disk_breads = None
    total_disk_bwrites = None
    total_net_in_kBps = None
    total_net_out_kBps = None
    net_out_kBps_time_series = None     # <timestamp, net tx per sec> dict, for network cdf plots
    per_stage_net_in_kBps = {}          # <stage, total net rx per node>
    per_stage_net_out_kBps = {}         # <stage, total net rx per node>


# Find spark stage for a timestamp given a dict with start and end times of all stages
def find_spark_stage(stages_start_end_times, timestamp):
    for stage, (stime, etime) in stages_start_end_times.items():
        if stime <= timestamp <= etime:
            return str(stage)
    return "None"


def get_metrics_summary_for_experiment(experiment_id, experiment_setup):
    experiment_dir_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
    print("Parsing experiment {0}".format(experiment_id))

    # Read setup file to get experiment parameters
    # setup_file_path = os.path.join(experiment_dir_path, "setup_details.txt")
    # experiment_setup = ExperimentSetup(setup_file_path)

    # Read stages and other timestamps from spark log file
    all_timestamp_list = []
    stages_start_end_times = {}
    spark_log_full_path = os.path.join(experiment_dir_path, experiment_setup.designated_driver_node,
                                       plot_one_experiment.spark_log_file_name)
    with open(spark_log_full_path, "r") as lines:
        for line in lines:
            matches = re.match(plot_one_experiment.spark_log_generic_log_regex, line)
            if matches:
                time_string = matches.group(1)
                timestamp = datetime.strptime(time_string, '%y/%m/%d %H:%M:%S')
                # timestamp = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')
                all_timestamp_list.append(timestamp)

            matches = re.match(plot_one_experiment.spark_stage_and_task_log_regex_2, line)
            if matches:
                time_string = matches.group(1)
                timestamp = datetime.strptime(time_string, '%y/%m/%d %H:%M:%S')
                # timestamp = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')

                # Keep track of start and end times of each stage
                stage = float(matches.group(2))
                if stage not in stages_start_end_times.keys():
                    stages_start_end_times[stage] = [timestamp, timestamp]
                stages_start_end_times[stage][1] = timestamp

    # If spark job start time and end time is not available, use the values from spark log file
    if experiment_setup.spark_job_start_time is None or experiment_setup.spark_job_end_time is None:
        experiment_setup.spark_job_start_time = min(all_timestamp_list)
        experiment_setup.spark_job_end_time = max(all_timestamp_list)

    # print(stages_start_end_times)
    per_node_metrics_dict = { node_name: ExperimentPerNodeMetrics() for node_name in experiment_setup.all_spark_nodes}

    '''
    # Get power file for each node and parse it
    all_power_readings = []
    power_readings_file_path = os.path.join(experiment_dir_path, experiment_setup.designated_driver_node,
                                            plot_one_experiment.power_readings_file_name)
    power_readings_counter = 0
    with open(power_readings_file_path, "r") as lines:
        for line in lines:
            matches = re.match(plot_one_experiment.power_regex, line)
            if matches:
                timestamp = datetime.fromtimestamp(float(matches.group(1)))
                power_readings_counter += 1

                i = 0
                for node_name in experiment_setup.power_meter_nodes_in_order:
                    power_watts = float(matches.group(i + 2))
                    all_power_readings.append([timestamp.replace(microsecond=0), node_name, "power_watts", power_watts])
                    i += 1

    # Simple sanity check: Alert if total number of power readings does not exceed experiment duration
    if power_readings_counter + 10 < (experiment_setup.spark_job_end_time - experiment_setup.spark_job_start_time).seconds:
        print("Number of power readings does not match experiment duration for Experiment {0}!".format(experiment_id))
        # raise Exception("Number of power readings does not match experiment duration for Experiment {0}!".format(experiment_id))
        return None

    # Filter power readings outside of the spark job time range
    all_power_readings = list(filter(lambda r: experiment_setup.spark_job_start_time < r[0] < experiment_setup.spark_job_end_time, all_power_readings))

    # Calculate total power consumed by each node, (in each spark 
    # stage) and add details to metrics
    for node_name in experiment_setup.power_meter_nodes_in_order:
        all_readings_node = list(filter(lambda r: r[1] == node_name, all_power_readings))
        total_power_consumed = sum(map(lambda r: r[3], all_readings_node))

        all_stages = sorted(set(map(lambda s: s[0], stages_time_stamp_list)))
        stages_power_list = {}
        for stage in all_stages:
            current_stage_time_stamp_list = list(filter(lambda s: s[0] == stage, stages_time_stamp_list))
            stage_start = min(map(lambda r: r[1], current_stage_time_stamp_list))
            stage_end = max(map(lambda r: r[1], current_stage_time_stamp_list))
            all_readings_stage = filter(lambda r: stage_start < r[0] < stage_end, all_readings_node)
            power_consumed_stage = sum(map(lambda r: r[3], all_readings_stage))
            stages_power_list.update({int(stage): power_consumed_stage})

        per_node_metrics_dict[node_name].total_power_consumed = total_power_consumed
        per_node_metrics_dict[node_name].per_stage_power_list = stages_power_list
    '''

    # Get disk usage on each node
    for node_name in experiment_setup.all_spark_nodes:
        diskio_full_path = os.path.join(experiment_dir_path, node_name, plot_one_experiment.diskio_readings_file_name)
        sum_disk_breads = 0
        sum_disk_bwrites = 0
        with open(diskio_full_path, "r") as lines:
            first_line = True
            date_part = None
            previous_reading_time_part = None
            for line in lines:
                if first_line:
                    date_string = plot_one_experiment.parse_date_from_sar_file(first_line_in_file=line)
                    date_part = datetime.strptime(date_string, '%m/%d/%Y')
                    first_line = False

                matches = re.match(plot_one_experiment.io_regex, line)
                if matches:
                    time_string = matches.group(1)

                    # Add a day when experiment runs past midnight, when the hour of the first reading is smaller than the one before.
                    time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                    if previous_reading_time_part is not None and previous_reading_time_part.hour > time_part.hour:
                        date_part = date_part + timedelta(days=1)
                    previous_reading_time_part = time_part
                    timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                    
                    disk_rps = float(matches.group(3))
                    disk_wps = float(matches.group(4))
                    disk_brps = float(matches.group(5))
                    disk_bwps = float(matches.group(6))
                    
                    if experiment_setup.spark_job_start_time < timestamp < experiment_setup.spark_job_end_time:
                        sum_disk_breads += disk_brps
                        sum_disk_bwrites += disk_bwps

        per_node_metrics_dict[node_name].total_disk_breads = sum_disk_breads
        per_node_metrics_dict[node_name].total_disk_bwrites = sum_disk_bwrites

    # Parse network usage on each node
    for node_name in experiment_setup.all_spark_nodes:
        net_full_path = os.path.join(experiment_dir_path, node_name, plot_one_experiment.net_readings_file_name)
        sum_net_in_kBps = 0
        sum_net_out_kBps = 0
        per_stage_net_in_kBps = {}
        per_stage_net_out_kBps = {}
        net_out_kBps_time_series = {}

        with open(net_full_path, "r") as lines:
            first_line = True
            date_part = None
            previous_reading_time_part = None
            current_spark_stage = None
            for line in lines:
                if first_line:
                    date_string = plot_one_experiment.parse_date_from_sar_file(first_line_in_file=line)
                    date_part = datetime.strptime(date_string, '%m/%d/%Y')
                    first_line = False

                matches = re.match(plot_one_experiment.network_regex, line)
                if matches:
                    time_string = matches.group(1)

                    # Timestamp could be in a different format, normalize it.
                    # Add a day when experiment runs past midnight, when the hour of the first reading is smaller than the one before.
                    time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                    if previous_reading_time_part is not None and previous_reading_time_part.hour > time_part.hour:
                        date_part = date_part + timedelta(days=1)
                    previous_reading_time_part = time_part
                    timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                    net_interface = matches.group(2)

                    # Taking enp59s0 or lo interface for now.
                    if net_interface == "enp59s0" and (experiment_setup.spark_job_start_time < timestamp < experiment_setup.spark_job_end_time):
                        net_in_KBps = float(matches.group(5))
                        net_out_KBps = float(matches.group(6))
                        sum_net_in_kBps += net_in_KBps
                        sum_net_out_kBps += net_out_KBps

                        # Record net tx individual measurements for network cdf plot
                        net_out_kBps_time_series[timestamp] = net_out_KBps

                        # Aggregate network usage in each spark stage
                        current_spark_stage = find_spark_stage(stages_start_end_times, timestamp)
                        # print(timestamp, current_spark_stage, net_in_KBps, net_out_KBps)
                        if current_spark_stage not in per_stage_net_in_kBps:    per_stage_net_in_kBps[current_spark_stage] = net_in_KBps
                        else: per_stage_net_in_kBps[current_spark_stage] += net_in_KBps
                        if current_spark_stage not in per_stage_net_out_kBps:   per_stage_net_out_kBps[current_spark_stage] = net_out_KBps
                        else: per_stage_net_out_kBps[current_spark_stage] += net_out_KBps            

        per_node_metrics_dict[node_name].total_net_in_kBps = sum_net_in_kBps
        per_node_metrics_dict[node_name].total_net_out_kBps = sum_net_out_kBps
        per_node_metrics_dict[node_name].per_stage_net_in_kBps = per_stage_net_in_kBps
        per_node_metrics_dict[node_name].per_stage_net_out_kBps = per_stage_net_out_kBps
        per_node_metrics_dict[node_name].net_out_kBps_time_series = net_out_kBps_time_series

    # Get accurate spark job times from detailed spark log if available
    spark_full_log_full_path = os.path.join(experiment_dir_path, experiment_setup.designated_driver_node, plot_one_experiment.spark_full_log_file_name)

    precise_start_time = None
    precise_end_time = None
    if os.path.exists(spark_log_full_path):
        with open(spark_full_log_full_path, "r") as lines:
            for line in lines:
                if 'SparkListenerJobStart' in line:
                    json_dict = json.loads(line)
                    precise_start_time = float(json_dict['Submission Time'])
                if 'SparkListenerJobEnd' in line:
                    json_dict = json.loads(line)
                    precise_end_time = float(json_dict['Completion Time'])


    exp_metrics = ExperimentMetrics(experiment_id, experiment_setup, per_node_metrics_dict)
    exp_metrics.stages_start_end_times = stages_start_end_times
    if precise_start_time and precise_end_time: exp_metrics.precise_total_time = (precise_end_time - precise_start_time)/1000
    return exp_metrics



def plot_total_power_usage_per_run_type(run_id, exp_metrics_list, output_dir, experiment_type, node_name=None):
    """
    Plots total power usage for different input sizes from experiments of same type (same experimental setup).
    If node_name is not None, filters for power usage of a single (specified) node.
    """

    fig, ax = plt.subplots(1,1)
    fig.suptitle("Energy consumption for '{0}' ".format(experiment_type) +
                 ("on node {0}".format(node_name) if node_name else " - all nodes"))
    ax.set_xlabel("Link bandwidth Mbps")
    ax.set_ylabel("Energy (watt-hours)")

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.experiment_setup.setup_type == experiment_type]
    all_sizes = sorted(set(map(lambda r: r.input_size_gb, exp_metrics_list)))
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_power_readings = []
        std_power_readings = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered))

            power_values = []
            for experiment in link_filtered:
                if node_name is not None:
                    power_values.append(experiment.per_node_metrics_dict[node_name].total_power_consumed/3600)
                else:
                    power_all_nodes = sum([n.total_power_consumed/3600
                                           for n in experiment.per_node_metrics_dict.values()
                                           if n.total_power_consumed is not None])
                    power_values.append(power_all_nodes)

            # avg_power_readings.append(math.log(np.mean(power_values)))
            avg_power_readings.append(np.mean(power_values))
            std_power_readings.append(np.std(power_values))
            # print(size, link_rate, ':'.join(power_values), np.mean(power_values), np.std(power_values), sep=", ")

        ax.errorbar(all_link_rates, avg_power_readings, std_power_readings, label='{0} GB'.format(size), marker="x")

    plt.legend()
    # plt.show()

    output_plot_file_name = "power_usage_{0}_wh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_total_power_usage_per_input_size(run_id, exp_metrics_list, output_dir, input_size_gb, node_name=None):
    """
    Plots total power usage for one input size across different experimental setups.
    If node_name is not None, filters for power usage of a single (specified) node.
    """

    fig, ax = plt.subplots(1,1)
    fig.suptitle("Energy consumption for '{0}' GB input ".format(input_size_gb) +
                 ("on node {0}".format(node_name) if node_name else " - all nodes"))
    ax.set_xlabel("Link bandwidth Mbps")
    ax.set_ylabel("Energy (watt-hours)")

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.input_size_gb == input_size_gb]
    all_exp_types = sorted(set(map(lambda r: r.experiment_setup.setup_type, exp_metrics_list)))
    for exp_type in all_exp_types:
        exp_type_filtered = list(filter(lambda r: r.experiment_setup.setup_type == exp_type, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, exp_type_filtered)))
        avg_power_readings = []
        std_power_readings = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, exp_type_filtered))

            power_values = []
            for experiment in link_filtered:
                if node_name is not None:
                    power_values.append(experiment.per_node_metrics_dict[node_name].total_power_consumed/3600)
                else:
                    power_all_nodes = round(sum([n.total_power_consumed/3600
                                           for n in experiment.per_node_metrics_dict.values()
                                           if n.total_power_consumed is not None]), 2)
                    power_values.append(power_all_nodes)

            # avg_power_readings.append(math.log(np.mean(power_values)))
            avg_power_readings.append(np.mean(power_values))
            std_power_readings.append(np.std(power_values))
            # print(input_size_gb, link_rate, power_values, round(np.mean(power_values), 2), round(np.std(power_values), 2), sep=", ")

        ax.errorbar(all_link_rates, avg_power_readings, std_power_readings, label='{0}'.format(exp_type), marker="x")

    plt.legend()
    # plt.show()

    output_plot_file_name = "power_usage_{0}_wh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_total_disk_usage_by_run_type(run_id, exp_metrics_list, output_dir, experiment_type, node_name=None):
    """
    Plots total disk usage for different input sizes from experiments of same type (same experimental setup).
    If node_name is not None, filters for power usage of a single (specified) node.
    """

    fig, (ax1, ax2) = plt.subplots(2, 1)
    fig.suptitle("Disk usage for {0} ".format(experiment_type) +
                 ("on node {0}".format(node_name) if node_name else " - all nodes"))

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.experiment_setup.setup_type == experiment_type]
    all_sizes = sorted(set(map(lambda r: r.input_size_gb, exp_metrics_list)))
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_disk_reads = []
        std_disk_reads = []
        avg_disk_writes = []
        std_disk_writes = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered))

            disk_reads = []
            disk_writes = []
            for experiment in link_filtered:
                if node_name is not None:
                    disk_reads.append(experiment.per_node_metrics_dict[node_name].total_disk_breads)
                    disk_writes.append(experiment.per_node_metrics_dict[node_name].total_disk_bwrites)
                else:
                    reads_all_nodes = sum([n.total_disk_breads for n in experiment.per_node_metrics_dict.values()])
                    writes_all_nodes = sum([n.total_disk_bwrites for n in experiment.per_node_metrics_dict.values()])
                    disk_reads.append(reads_all_nodes)
                    disk_writes.append(writes_all_nodes)

            avg_disk_reads.append(np.mean(disk_reads) * 512/(1024*1024))
            std_disk_reads.append(np.std(disk_reads) * 512/(1024*1024))
            avg_disk_writes.append(np.mean(disk_writes) * 512/(1024*1024))
            std_disk_writes.append(np.std(disk_writes) * 512/(1024*1024))


        ax1.set_xlabel("Link bandwidth Mbps")
        ax1.set_ylabel("Total Disk Reads MB")
        ax1.errorbar(all_link_rates, avg_disk_reads, std_disk_reads, label='{0} GB'.format(size), marker="x")
        ax2.set_xlabel("Link bandwidth Mbps")
        ax2.set_ylabel("Total Disk Writes MB")
        ax2.errorbar(all_link_rates, avg_disk_writes, std_disk_writes, label='{0} GB'.format(size), marker=".")

    plt.legend()
    # plt.show()

    output_plot_file_name = "disk_usage_{0}_kwh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_total_disk_usage_by_input_size(run_id, exp_metrics_list, output_dir, input_size_gb, node_name=None):
    """
    Plots total disk usage for one input size across different experimental setups.
    If node_name is not None, filters for power usage of a single (specified) node.
    """

    fig, (ax1, ax2) = plt.subplots(2, 1)
    fig.suptitle("Disk usage for {0} GB input".format(input_size_gb) +
                 ("on node {0}".format(node_name) if node_name else " - all nodes"))

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.input_size_gb == input_size_gb]
    all_exp_types = sorted(set(map(lambda r: r.experiment_setup.setup_type, exp_metrics_list)))
    for exp_type in all_exp_types:
        exp_type_filtered = list(filter(lambda r: r.experiment_setup.setup_type == exp_type, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, exp_type_filtered)))
        avg_disk_reads = []
        std_disk_reads = []
        avg_disk_writes = []
        std_disk_writes = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, exp_type_filtered))

            disk_reads = []
            disk_writes = []
            for experiment in link_filtered:
                if node_name is not None:
                    disk_reads.append(experiment.per_node_metrics_dict[node_name].total_disk_breads)
                    disk_writes.append(experiment.per_node_metrics_dict[node_name].total_disk_bwrites)
                else:
                    reads_all_nodes = round(sum([n.total_disk_breads for n in experiment.per_node_metrics_dict.values()])* 512/(1024*1024), 2)
                    writes_all_nodes = round(sum([n.total_disk_bwrites for n in experiment.per_node_metrics_dict.values()])* 512/(1024*1024), 2)
                    disk_reads.append(reads_all_nodes)
                    disk_writes.append(writes_all_nodes)

            print(input_size_gb, link_rate, disk_reads, round(np.mean(disk_reads), 2), round(np.std(disk_reads), 2), sep=", ")
            avg_disk_reads.append(np.mean(disk_reads))
            std_disk_reads.append(np.std(disk_reads))
            avg_disk_writes.append(np.mean(disk_writes))
            std_disk_writes.append(np.std(disk_writes))

        ax1.set_xlabel("Link bandwidth Mbps")
        ax1.set_ylabel("Total Disk Reads MB")
        ax1.errorbar(all_link_rates, avg_disk_reads, std_disk_reads, label='{0}'.format(exp_type), marker="x")
        ax2.set_xlabel("Link bandwidth Mbps")
        ax2.set_ylabel("Total Disk Writes MB")
        ax2.errorbar(all_link_rates, avg_disk_writes, std_disk_writes, label='{0}'.format(exp_type), marker=".")

    plt.legend()
    # plt.show()

    # output_plot_file_name = "disk_usage_{0}_kwh_{1}.png".format(node_name if node_name else "total", run_id)
    # output_full_path = os.path.join(output_dir, output_plot_file_name)
    # plt.savefig(output_full_path)


def plot_total_network_usage_by_run_type(run_id, exp_metrics_list, output_dir, experiment_type, node_name=None):
    """
    Plots total network usage for different input sizes from experiments of same type (same experimental setup).
    If node_name is not None, filters for network usage of a single (specified) node.
    """

    fig, (ax1, ax2) = plt.subplots(2, 1)
    fig.suptitle("Network usage for {0} ".format(experiment_type) +
                 ("on node {0}".format(node_name) if node_name else " - all nodes"))

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.experiment_setup.setup_type == experiment_type]
    all_sizes = sorted(set(map(lambda r: r.input_size_gb, exp_metrics_list)))
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_net_inKbps = []
        std_net_inKbps = []
        avg_net_outKbps = []
        std_net_outKbps = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered))

            net_inKbps = []
            net_outKbps = []
            for experiment in link_filtered:
                if node_name is not None:
                    net_inKbps.append(experiment.per_node_metrics_dict[node_name].total_net_in_kBps)
                    net_outKbps.append(experiment.per_node_metrics_dict[node_name].total_net_out_kBps)
                else:
                    net_inKbps_all_nodes = sum([n.total_net_in_kBps for n in experiment.per_node_metrics_dict.values()])
                    net_outKbps_all_nodes = sum([n.total_net_out_kBps for n in experiment.per_node_metrics_dict.values()])
                    net_inKbps.append(net_inKbps_all_nodes)
                    net_outKbps.append(net_outKbps_all_nodes)

            avg_net_inKbps.append(np.mean(net_inKbps) / (1024*1024))
            std_net_inKbps.append(np.std(net_inKbps) / (1024*1024))
            avg_net_outKbps.append(np.mean(net_outKbps) / (1024*1024))
            std_net_outKbps.append(np.std(net_outKbps) / (1024*1024))


        ax1.set_xlabel("Link bandwidth Mbps")
        ax1.set_ylabel("Total Network In GB")
        ax1.errorbar(all_link_rates, avg_net_inKbps, std_net_inKbps, label='{0} GB'.format(size), marker="x")
        ax2.set_xlabel("Link bandwidth Mbps")
        ax2.set_ylabel("Total Network Out GB")
        ax2.errorbar(all_link_rates, avg_net_outKbps, std_net_outKbps, label='{0} GB'.format(size), marker=".")

    plt.legend()
    # plt.show()

    output_plot_file_name = "network_usage_{0}_kwh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_total_network_usage_by_input_size(run_id, exp_metrics_list, output_dir, input_size_gb, node_name=None):
    """
    Plots total network usage for one input size across different experimental setups.
    If node_name is not None, filters for network usage of a single (specified) node.
    """

    fig, (ax1, ax2) = plt.subplots(2, 1)
    fig.suptitle("Network usage for {0} GB input".format(input_size_gb) +
                 ("on node {0}".format(node_name) if node_name else " - all nodes"))

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.input_size_gb == input_size_gb]
    all_exp_types = sorted(set(map(lambda r: r.experiment_setup.setup_type, exp_metrics_list)))
    for exp_type in all_exp_types:
        exp_type_filtered = list(filter(lambda r: r.experiment_setup.setup_type == exp_type, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, exp_type_filtered)))
        avg_net_inKbps = []
        std_net_inKbps = []
        avg_net_outKbps = []
        std_net_outKbps = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, exp_type_filtered))

            net_inKbps = []
            net_outKbps = []
            for experiment in link_filtered:
                if node_name is not None:
                    net_inKbps.append(experiment.per_node_metrics_dict[node_name].total_net_in_kBps)
                    net_outKbps.append(experiment.per_node_metrics_dict[node_name].total_net_out_kBps)
                else:
                    net_inKbps_all_nodes = sum([n.total_net_in_kBps for n in experiment.per_node_metrics_dict.values()])
                    net_outKbps_all_nodes = sum([n.total_net_out_kBps for n in experiment.per_node_metrics_dict.values()])
                    net_inKbps.append(net_inKbps_all_nodes)
                    net_outKbps.append(net_outKbps_all_nodes)

            avg_net_inKbps.append(np.mean(net_inKbps) / (1024*1024))
            std_net_inKbps.append(np.std(net_inKbps) / (1024*1024))
            avg_net_outKbps.append(np.mean(net_outKbps) / (1024*1024))
            std_net_outKbps.append(np.std(net_outKbps) / (1024*1024))

        ax1.set_xlabel("Link bandwidth Mbps")
        ax1.set_ylabel("Total Network In GB")
        ax1.errorbar(all_link_rates, avg_net_inKbps, std_net_inKbps, label='{0}'.format(exp_type), marker="x")
        ax2.set_xlabel("Link bandwidth Mbps")
        ax2.set_ylabel("Total Network Out GB")
        ax2.errorbar(all_link_rates, avg_net_outKbps, std_net_outKbps, label='{0}'.format(exp_type), marker=".")

    plt.legend()
    # plt.show()

    output_plot_file_name = "network_usage_{0}_kwh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_cdf_network_throughput(run_id, exp_metrics_list, output_dir, node_name=None):
    """
    Plots cdf lines for network throughput rates for given experiments.
    If node_name is not None, filters for network throughput observed on a single (specified) node.
    """

    fig, ax = plt.subplots(1, 1)
    # fig.set_size_inches(w=10,h=10)
    fig.suptitle("CDF of network readings (reduce phase) " +
                 ("on node {0}".format(node_name) if node_name else "on all nodes"))

    for exp in exp_metrics_list:
        if exp.experiment_setup.scala_class_name == "SortNoDisk":
            continue
        
        # Just plot the reduce phase, as no network happens in map phase
        reduce_phase = exp.stages_start_end_times[1]
        net_out_readings_kBps = exp.per_node_metrics_dict[node_name].net_out_kBps_time_series.items() if node_name \
            else [reading for p in exp.per_node_metrics_dict.values() for reading in p.net_out_kBps_time_series.items()]
        net_out_readings_kBps_reduce_phase = [v for k,v in net_out_readings_kBps if reduce_phase[0] < k < reduce_phase[1]]

        # print("Total net tx kB: " + str(sum(net_out_readings_kBps_reduce_phase)))
        net_out_readings_mbps = [r*8/1024 for r in net_out_readings_kBps_reduce_phase]
        cdf_x, cdf_y = plot_one_experiment.gen_cdf_curve(net_out_readings_mbps, 1000)
        # cdf_x, cdf_y = plot_one_experiment.gen_cumsum_curve(net_out_readings_mbps, 1000)

        ax.set_xlabel("Network throughput mbps")
        ax.set_ylabel("Cumulative Count")
        ax.plot(cdf_x, cdf_y, label='{0}'.format(exp.get_plot_friendly_name()))

        # if exp.experiment_setup.scala_class_name == "SortNoDisk":
        #     reduce_phase = exp.stages_start_end_times[2]
        #     net_out_readings_kBps = [reading for p in exp.per_node_metrics_dict.values() \
        #                             for time, reading in p.net_out_kBps_time_series.items() \
        #                             if reduce_phase[0] < time < reduce_phase[1]]                
        #     print("Total net tx kB (reduce phase): " + str(sum(net_out_readings_kBps)))
        #     net_out_readings_mbps = [r*8/1024 for r in net_out_readings_kBps]
        #     cdf_x, cdf_y = plot_one_experiment.gen_cdf(net_out_readings_mbps, 1000)
        #     ax.set_xlabel("Network rate mbps")
        #     ax.set_ylabel("CDF")
        #     ax.plot(cdf_x, cdf_y, label='SortNoDisk (Reduce)')

    plt.legend()
    # plt.show()

    output_plot_file_name = "network_cdf_{0}_mB_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_exp_duration_per_run_type(run_id, exp_metrics_list, output_dir, experiment_group):
    """
    Plots experiment duration for different input sizes from experiments of same type (same experimental setup).
    """

    fig, ax = plt.subplots(1,1)
    fig.suptitle("Experiment duration ({0})".format(experiment_group))
    ax.set_xlabel("Link bandwidth Mbps")
    ax.set_ylabel("Duration (secs)")

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.experiment_setup.experiment_group == experiment_group]
    all_sizes = sorted(set(map(lambda r: r.input_size_gb, exp_metrics_list)))
    avg_durations = []
    std_durations = []
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, exp_metrics_list))
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_durations = []
        std_durations = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered))
            all_exp_durations = [e.duration.total_seconds() for e in link_filtered]
            # avg_power_readings.append(math.log(np.mean(power_values)))
            avg_durations.append(np.mean(all_exp_durations))
            std_durations.append(np.std(all_exp_durations))
        
        ax.errorbar(all_link_rates, avg_durations, std_durations, label='Size: {0} GB'.format(size), marker="x")

    plt.legend()
    # plt.show()

    output_plot_file_name = "duration_{0}.png".format(run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_exp_duration_per_input_size(run_id, exp_metrics_list, output_dir, input_size_gb):
    """
    Plots experiment duration for one input size across different experimental setups.
    """

    fig, ax = plt.subplots(1,1)
    fig.suptitle("Experiment duration ({0} GB)  ".format(input_size_gb))
    ax.set_xlabel("Link bandwidth Mbps")
    ax.set_ylabel("Duration (secs)")

    exp_metrics_list = [exp for exp in exp_metrics_list if exp.input_size_gb == input_size_gb]
    all_exp_types = sorted(set(map(lambda r: r.experiment_setup.experiment_group, exp_metrics_list)))
    for exp_type_id in all_exp_types:
        exp_type_filtered = list(filter(lambda r: r.experiment_setup.experiment_group == exp_type_id, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, exp_type_filtered)))
        avg_durations = []
        std_durations = []
        for link_rate in all_link_rates:
            link_filtered = list(filter(lambda f: f.link_bandwidth_mbps == link_rate, exp_type_filtered))
            all_exp_durations = [e.duration.total_seconds() for e in link_filtered]
            # avg_durations.append(math.log(np.mean(all_exp_durations)))
            avg_durations.append(np.mean(all_exp_durations))
            std_durations.append(np.std(all_exp_durations))
            # print(round(math.log(link_rate), 2), round(math.log(np.mean(all_exp_durations)), 2), sep=", ")

        log_link_rates = [math.log(l) for l in all_link_rates]
        ax.errorbar(all_link_rates, avg_durations, std_durations, label='Exp group: {0}'.format(exp_type_id), marker="x")
    
    plt.legend()
    # plt.show()

    output_plot_file_name = "duration_{0}.png".format(run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


# Loads all experiment results
def load_all_experiments(start_time, end_time):
    experiments = []

    all_experiment_folders = [os.path.join(plot_one_experiment.results_base_dir, item)
                       for item in os.listdir(plot_one_experiment.results_base_dir)
                       if item.startswith("Exp-")
                       and os.path.isdir(os.path.join(plot_one_experiment.results_base_dir, item))]

    for experiment_dir_path in all_experiment_folders:
        experiment_time = datetime.fromtimestamp(os.path.getctime(experiment_dir_path))
        if start_time < experiment_time < end_time:
            experiment_id = os.path.basename(experiment_dir_path)
            # print("Loading " + experiment_id)
            setup_file_path = os.path.join(experiment_dir_path, "setup_details.txt")
            experiment_setup = ExperimentSetup(setup_file_path)
            experiment_setup.experiment_id = experiment_id
            if start_time < experiment_setup.experiment_start_time < end_time:
                experiments.append(experiment_setup)

    return sorted(experiments, key=lambda x: x.experiment_start_time, reverse=True)


# Filters
power_plots_output_dir = plot_one_experiment.results_base_dir + "\\PowerPlots\\" + datetime.now().strftime("%m-%d")
global_start_time = datetime.strptime('2019-04-30 00:00:00', "%Y-%m-%d %H:%M:%S")
global_end_time = datetime.now()
filter_experiments = True
experiments_filter = [
    # "Run-2019-06-10-17-27-05",       # Locality wait 100s, analyzing the variation in network xput across nodes
    # "Run-2019-06-10-22-21-08", "Run-2019-06-11-00-08-22"    # Locality wait 0s, no numa vs use numa optimization option
    # "Run-2019-06-11-12-25-43", "Run-2019-06-11-12-33-16"   # Locality wait 100s, use numa vs no numa optimization option
    # "Run-2019-06-11-13-24-12", "Run-2019-06-11-13-31-23"   # Two executors per node
    
    # "Exp-2019-06-13-18-12-52", "Exp-2019-06-13-18-14-25",     # 300gb sort; 1sec locality wait; 10 vs 40gbps after perfect hdfs file distribution
    # "Exp-2019-06-13-18-53-16", "Exp-2019-06-13-18-54-36"      # 200gb sort; 1sec locality wait; 10 vs 40gbps after perfect hdfs file distribution
    "Exp-2019-07-10-13-56-23", "Exp-2019-07-10-13-52-47", "Exp-2019-07-10-13-31-29", "Exp-2019-07-10-13-37-28", "Exp-2019-07-10-17-14-23",
]
input_sizes_filter = [20, 40, 60, 80, 100, 200, 300]
link_rates_filter = [10000, 40000]
# input_sizes_filter = [40]
# link_rates_filter = [200]

# The results directory is full of folders corresponding to all the experiments ever performed.
# This one filters relevant experiments to parse and plot.
def filter_experiments_to_consider(all_experiments):
    experiments_to_consider = []

    for experiment in all_experiments:
        if (not filter_experiments) or experiment.experiment_id in experiments_filter \
            or experiment.experiment_group in experiments_filter:
            if experiment.input_size_gb in input_sizes_filter:
                if experiment.link_bandwidth_mbps in link_rates_filter:
                    # print(experiment.input_size_gb, experiment.link_bandwidth_mbps)
                    experiments_to_consider.append(experiment)

    return experiments_to_consider


# Print some statistics on total network throughput in spark stages
def print_network_usage_stats(exp_result):
    exp = exp_result
    

    net_tx_by_stages_on_each_node = [(node, stage, net_tx) for (node, val) in exp.per_node_metrics_dict.items() for (stage, net_tx) in val.per_stage_net_out_kBps.items()]
    net_rx_by_stages_on_each_node = [(node, stage, net_rx) for (node, val) in exp.per_node_metrics_dict.items() for (stage, net_rx) in val.per_stage_net_in_kBps.items()]
    all_nodes = exp.per_node_metrics_dict.keys()   
    all_stages = set([s[1] for s in net_tx_by_stages_on_each_node])

    # Network usage breakdown by stage, aggregated over all nodes
    print("Total TX (GB) all nodes: {:.2f}".format(exp.total_net_out_KB_all_nodes/(1024*1024)))
    print("Total RX (GB) all nodes: {:.2f}".format(exp.total_net_in_KB_all_nodes/(1024*1024)))
    for stage in sorted(all_stages):
        net_tx_by_stage = [s[2]/(1024*1024) for s in net_tx_by_stages_on_each_node if s[1] == stage]
        net_rx_by_stage = [s[2]/(1024*1024) for s in net_rx_by_stages_on_each_node if s[1] == stage]
        print("Stage ", stage, "TX (GB): Total: {:.2f} GB, Per Node: {:.2f} ({:.2f}) GB".format(np.sum(net_tx_by_stage), np.mean(net_tx_by_stage), np.std(net_tx_by_stage)))
        print("Stage ", stage, "RX (GB): Total: {:.2f} GB, Per Node: {:.2f} ({:.2f}) GB".format(np.sum(net_rx_by_stage), np.mean(net_rx_by_stage), np.std(net_rx_by_stage)))

    # Network usage breakdown by stage, for each node
    for node_name in all_nodes:
        net_tx_per_node_by_stage = sorted([(stage, sum([s[2] for s in net_tx_by_stages_on_each_node if s[0] == node_name and s[1] == stage])) for stage in all_stages])
        net_rx_per_node_by_stage = sorted([(stage, sum([s[2] for s in net_rx_by_stages_on_each_node if s[0] == node_name and s[1] == stage])) for stage in all_stages])
        # print(node_name, "TX (GB)", [(k, round(v/(1024*1024),2)) for k,v in net_tx_per_node_by_stage])
        # print(node_name, "RX (GB)", [(k, round(v/(1024*1024),2)) for k,v in net_rx_per_node_by_stage])


# Print some relevant stats from collected metrics
def print_stats(all_results, power_plots_output_dir):
    all_partition_counts = sorted(set([r.experiment_setup.final_partition_count for r in all_results]))
    for exp in all_results:
        # duration_str = "Run time: {0} secs".format(round(exp.duration.total_seconds(), 2))
        duration_str = "Run time: {:.2f} secs".format(exp.precise_total_time)
        stages_start_end_times_str = ", ".join([ "{0}: {1}".format(k, round((v[1] - v[0]).seconds, 1)) for k,v in exp.stages_start_end_times.items()])
        print("{:<25s} {:<25s} {:<30s}{:<50s}".format( exp.experiment_id, duration_str, stages_start_end_times_str, exp.experiment_setup.experiment_group_desc))
        # print("{:<25s} {:<30s} {:<25s} {:<100s}".format(duration_str, stages_start_end_times_str, exp.experiment_id, exp.experiment_setup.plot_friendly_name))
        # print("{0} ({1})".format(str(round(exp.total_power_all_nodes/3600, 2)), str(round(exp.duration.seconds/60, 1))))
        # print("{0} {1} {2}".format(exp.link_bandwidth_mbps, str(round(exp.total_power_all_nodes/3600, 2)), str(round(exp.duration.seconds/60, 1)), sep=", "))
         
        # print_network_usage_stats(exp)

        # And copy the experiment to power plots output
        # plots_dir_path = plot_one_experiment.parse_and_plot_results(exp_id)
        # shutil.copytree(plots_dir_path, os.path.join(power_plots_output_dir, exp_id))
        pass

    # Print job times per each link bandwidth
    # for partition_count in all_partition_counts:
    #     for link_rate in link_rates_filter:
    #         job_times = [round(exp.duration.total_seconds(), 2) for exp in all_results 
    #                         if exp.link_bandwidth_mbps == link_rate 
    #                             and exp.experiment_setup.final_partition_count == partition_count]
    #         print("{0}Gbps, partitions:{4} => Mean:{2} std:{3}, {1}".format(link_rate/1000, job_times, round(np.mean(job_times), 2), round(np.std(job_times), 1), partition_count))
            # print("{4} {0} {2} {3}".format(link_rate/1000, job_times, round(np.mean(job_times), 2), round(np.std(job_times), 1), partition_count))


def main():

    # Parse results
    all_experiments = load_all_experiments(global_start_time, global_end_time)
    relevant_experiments = filter_experiments_to_consider(all_experiments)
    all_results = [get_metrics_summary_for_experiment(exp.experiment_id, exp) for exp in relevant_experiments]
    # print(all_results)

    print("Output plots at path: " + power_plots_output_dir)
    if not os.path.exists(power_plots_output_dir):
        os.mkdir(power_plots_output_dir)

    # Parse args and call relevant action
    parser = argparse.ArgumentParser("Generates different kinds of plots from results across different experiments")
    parser.add_argument('--printstats', action='store_true', help='Prints some experiment aggregate statistics like duration, total network usage, etc.')
    parser.add_argument('--all', action='store_true', help='Generates all kinds of plots available')
    parser.add_argument('--power', action='store_true', help='Generates plots for total power consumed for specified runs')
    parser.add_argument('--network', action='store_true', help='Generates plots for total network usage for specified runs')
    parser.add_argument('--diskio', action='store_true', help='Generates plots for total disk usage for specified runs')
    parser.add_argument('--runtime', action='store_true', help='Generates plots for job execution times for specified runs')
    parser.add_argument('--netcdf', action='store_true', help='Generates a cdf plot for network tx throughput for specified runs')
    args = parser.parse_args()

    # Print any stats we might want to look at
    if args.printstats:
        print_stats(all_results, power_plots_output_dir)

    if args.all or args.runtime:
        # Plot experiment duration by input size
        for size in input_sizes_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            plot_exp_duration_per_input_size(run_id, all_results, power_plots_output_dir, input_size_gb=size)
            pass

        # Plot experiment duration by experimental setup
        for exp_grp_id in experiment_groups_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            # plot_exp_duration_per_run_type(run_id, all_results, power_plots_output_dir, experiment_group=exp_grp_id)
            pass

    if args.all or args.power:
        # Plot power results per input size
        for size in input_sizes_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            plot_total_power_usage_per_input_size(run_id, all_results, power_plots_output_dir, input_size_gb=size)
            pass

        # Plot power results by experimental setup
        for exp_type in experiment_groups_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            # plot_total_power_usage_per_run_type(run_id, all_results, power_plots_output_dir, experiment_type=exp_type)
            pass

    if args.all or args.diskio:
        # Plot disk usage by input size
        for size in input_sizes_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            plot_total_disk_usage_by_input_size(run_id, all_results, power_plots_output_dir, input_size_gb=size)
            pass

        # Plot disk usage by experimental setup
        for exp_type in experiment_groups_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            # plot_total_disk_usage_by_run_type(run_id, all_results, power_plots_output_dir, experiment_type=exp_type)
            pass
        
    if args.all or args.network:
        # Plot netwokr usage by input size
        for size in input_sizes_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            plot_total_network_usage_by_input_size(run_id, all_results, power_plots_output_dir, input_size_gb=size)
            pass

        # Plot network usage by experimental setup
        for exp_type in experiment_groups_filter:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            # plot_total_network_usage_by_run_type(run_id, all_results, power_plots_output_dir, experiment_type=exp_type)
            pass

    if args.all or args.netcdf:
            run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            plot_cdf_network_throughput(run_id, all_results, power_plots_output_dir, node_name=None)
            pass


if __name__ == "__main__":
    main()

