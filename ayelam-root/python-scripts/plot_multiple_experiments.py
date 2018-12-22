"""
Aggregates power readings from multiple experiments and generates plots
"""

import os
import re
import math
from datetime import datetime
import matplotlib.pyplot as plt
import plot_one_experiment
from plot_one_experiment import ExperimentSetup
import numpy as np


class ExperimentMetrics:
    experiment_id = None
    experiment_setup = None
    experiment_start_time = None
    input_size_gb = None
    link_bandwidth_mbps = None
    duration = None
    total_power_all_nodes = None
    per_node_metrics_dict = None

    def __init__(self, experiment_id, experiment_setup, per_node_metrics_dict):
        self.experiment_id = experiment_id
        self.experiment_setup = experiment_setup
        self.experiment_start_time = experiment_setup.experiment_start_time
        self.input_size_gb = experiment_setup.input_size_gb
        self.link_bandwidth_mbps = experiment_setup.link_bandwidth_mbps
        self.duration = (experiment_setup.spark_job_end_time - experiment_setup.spark_job_start_time)
        self.per_node_metrics_dict = per_node_metrics_dict
        self.total_power_all_nodes = sum([n.total_power_consumed for n in per_node_metrics_dict.values()
                                          if n.total_power_consumed is not None])


class ExperimentPerNodeMetrics:
    total_power_consumed = None
    per_stage_power_list = []
    total_disk_breads = None
    total_disk_bwrites = None


def get_metrics_summary_for_experiment(experiment_id):

    experiment_dir_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
    print("Parsing experiment {0}".format(experiment_id))

    # Read setup file to get experiment parameters
    setup_file_path = os.path.join(experiment_dir_path, "setup_details.txt")
    experiment_setup = ExperimentSetup(setup_file_path)

    # Read stages and other timestamps from spark log file
    all_timestamp_list = []
    stages_time_stamp_list = []
    spark_log_full_path = os.path.join(experiment_dir_path, experiment_setup.designated_driver_node,
                                       plot_one_experiment.spark_log_file_name)
    with open(spark_log_full_path, "r") as lines:
        for line in lines:
            matches = re.match(plot_one_experiment.spark_log_generic_log_regex, line)
            if matches:
                time_string = matches.group(1)
                timestamp = datetime.strptime(time_string, '%y/%m/%d %H:%M:%S')
                all_timestamp_list.append(timestamp)

            matches = re.match(plot_one_experiment.spark_stage_and_task_log_regex, line)
            if matches:
                time_string = matches.group(1)
                timestamp = datetime.strptime(time_string, '%y/%m/%d %H:%M:%S')
                stage = float(matches.group(2))
                stages_time_stamp_list.append([stage, timestamp])

    # If spark job start time and end time is not available, use the values from spark log file
    if experiment_setup.spark_job_start_time is None or experiment_setup.spark_job_end_time is None:
        experiment_setup.spark_job_start_time = min(all_timestamp_list)
        experiment_setup.spark_job_end_time = max(all_timestamp_list)

    per_node_metrics_dict = { node_name: ExperimentPerNodeMetrics() for node_name in experiment_setup.all_spark_nodes}

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
        raise Exception("Number of power readings does not match experiment duration for Experiment {0}!".format(experiment_id))

    # Filter power readings outside of the spark job time range
    all_power_readings = list(filter(lambda r: experiment_setup.spark_job_start_time < r[0] < experiment_setup.spark_job_end_time, all_power_readings))

    # Calculate total power consumed by each node, (in each spark stage) and add details to metrics
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

    # Get disk usage on each node
    for node_name in experiment_setup.all_spark_nodes:
        diskio_full_path = os.path.join(experiment_dir_path, node_name, plot_one_experiment.diskio_readings_file_name)
        sum_disk_breads = 0
        sum_disk_bwrites = 0
        with open(diskio_full_path, "r") as lines:
            for line in lines:
                matches = re.match(plot_one_experiment.io_regex, line)
                if matches:
                    disk_rps = float(matches.group(3))
                    disk_wps = float(matches.group(4))
                    disk_brps = float(matches.group(5))
                    disk_bwps = float(matches.group(6))
                    sum_disk_breads += disk_brps
                    sum_disk_bwrites += disk_bwps

        per_node_metrics_dict[node_name].total_disk_breads = sum_disk_breads
        per_node_metrics_dict[node_name].total_disk_bwrites = sum_disk_bwrites

    return ExperimentMetrics(experiment_id, experiment_setup, per_node_metrics_dict)


def plot_total_power_usage(run_id, exp_metrics_list, output_dir, node_name=None):
    """
    Plots total power usage. If node_name is not None, filters for the node.
    """

    fig, ax = plt.subplots(1,1)
    fig.suptitle("Energy consumption (No optimizations)" + ("on node {0}".format(node_name) if node_name else " - all nodes"))
    ax.set_xlabel("Link bandwidth Mbps")
    ax.set_ylabel("Energy (watt-hours)")

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

        ax.errorbar(all_link_rates, avg_power_readings, std_power_readings, label='{0} GB'.format(size), marker="x")

    plt.legend()
    # plt.show()

    output_plot_file_name = "power_usage_{0}_wh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_total_disk_usage(run_id, exp_metrics_list, output_dir, node_name=None):
    """
    Plots total disk reads/writes. If node_name is not None, filters for the node.
    """

    fig, (ax1, ax2) = plt.subplots(2, 1)
    fig.suptitle("Disk usage (No optimizations)" + ("on node {0}".format(node_name) if node_name else " - all nodes"))

    all_sizes = sorted(set(map(lambda r: r.input_size_gb, exp_metrics_list)))
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, exp_metrics_list))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_disk_reads = []
        avg_disk_writes = []
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

            # avg_power_readings.append(math.log(np.mean(power_values)))
            avg_disk_reads.append(np.mean(disk_reads) * 512/(1024*1024))
            avg_disk_writes.append(np.mean(disk_writes) * 512/(1024*1024))

        ax1.set_xlabel("Link bandwidth Mbps")
        ax1.set_ylabel("Total Disk Reads MB")
        ax1.plot(all_link_rates, avg_disk_reads, label='{0} GB'.format(size), marker="x")
        ax2.set_xlabel("Link bandwidth Mbps")
        ax2.set_ylabel("Total Disk Writes MB")
        ax2.plot(all_link_rates, avg_disk_writes, label='{0} GB'.format(size), marker=".")

    plt.legend()
    # plt.show()

    output_plot_file_name = "disk_usage_{0}_kwh_{1}.png".format(node_name if node_name else "total", run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    plt.savefig(output_full_path)


def plot_experiment_duration(run_id, experiment_power_results, output_dir):
    """
    Takes list of ExperimentRunResult objects, one for power usage on each node in each experiment
    """

    fig, ax = plt.subplots(1,1)
    fig.set_size_inches(w=5,h=5)
    fig.suptitle("Experiment duration vs Link bandwidth")
    ax.set_xlabel("Link bandwidth Mbps")
    ax.set_ylabel("Duration (secs)")

    all_sizes = set(map(lambda r: r.input_size_gb, experiment_power_results))
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, experiment_power_results))
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_duration = []
        for link_rate in all_link_rates:
            filtered_readings = list(map(lambda m: m.duration.seconds,
                                               filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered)))
            avg_duration.append(np.mean(filtered_readings))
        ax.plot(all_link_rates, avg_duration, label='{0}GB'.format(size), marker='x')

    plt.legend()
    plt.show()

    output_plot_file_name = "duration_{0}.png".format(run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    # plt.savefig(output_full_path)


def filter_experiments_to_consider():
    # 12/10 22:00 to 12/11 14:00 ---> No output + HDFS caching
    # 12/06 00:00 to 12/08 00:00 ---> No output
    # 11/29 00:00 to 12/01 00:00 ---> Legacy sort
    # 12/21 00:00 to 12/22 00:00 ---> No output + Caching on one replica
    start_time = datetime.strptime('2018-12-10 22:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime('2018-12-11 14:00:00', '%Y-%m-%d %H:%M:%S')

    experiments_to_consider = []
    all_experiments = [os.path.join(plot_one_experiment.results_base_dir, item) for item in os.listdir(plot_one_experiment.results_base_dir)
               if item.startswith("Exp-") and os.path.isdir(os.path.join(plot_one_experiment.results_base_dir, item))]

    for experiment_dir_path in all_experiments:
        experiment_id = os.path.basename(experiment_dir_path)
        experiment_time = datetime.fromtimestamp(os.path.getctime(experiment_dir_path))
        if start_time < experiment_time < end_time:
            # Read setup file to get experiment parameters
            setup_file_path = os.path.join(experiment_dir_path, "setup_details.txt")
            experiment_setup = ExperimentSetup(setup_file_path)
            if experiment_setup.input_size_gb in [1, 5, 10, 50]:
                if experiment_setup.link_bandwidth_mbps in [200, 400, 600, 800, 1000]:
                    # print(experiment_id, experiment_setup.input_size_gb, experiment_setup.link_bandwidth_mbps)
                    experiments_to_consider.append(experiment_id)

    return experiments_to_consider


if __name__ == "__main__":
    power_plots_output_dir = 'D:\Power Measurements\PowerPlots\\12-21'
    run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # Get experiments to consider
    experiments_to_consider = filter_experiments_to_consider()
    print(experiments_to_consider)

    all_results = [get_metrics_summary_for_experiment(exp_id) for exp_id in experiments_to_consider]
    # print(all_results)

    # all_exps = set([p.experiment_id for p in all_results])
    # for exp in all_results:
    #    print("{0} ({1})".format(str(round(exp.total_power_all_nodes/3600, 2)), str(round(exp.duration.seconds/60, 1))))


    plot_total_power_usage(run_id, all_results, power_plots_output_dir)
    plot_total_disk_usage(run_id, all_results, power_plots_output_dir)
    # plot_experiment_duration(run_id, all_results, power_plots_output_dir)

