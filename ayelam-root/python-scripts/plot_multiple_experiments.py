"""
Aggregates power readings from multiple experiments and generated plots
"""

import os
import re
import math
from datetime import datetime
import matplotlib.pyplot as plt
import plot_one_experiment
from plot_one_experiment import ExperimentSetup
import numpy as np


# Constants
power_plots_output_dir = 'D:\Power Measurements\PowerPlots'


class ExperimentMetrics:
    experiment_id = None
    experiment_start_time = None
    input_size_gb = None
    link_bandwidth_mbps = None
    duration = None
    total_power_consumed = None
    per_node_metrics_list = None

    def __init__(self, experiment_id, experiment_setup, per_node_metrics_list):
        self.experiment_id = experiment_id
        self.experiment_start_time = experiment_setup.experiment_start_time
        self.input_size_gb = experiment_setup.input_size_gb
        self.link_bandwidth_mbps = experiment_setup.link_bandwidth_mbps
        self.duration = (experiment_setup.spark_job_end_time - experiment_setup.spark_job_start_time)
        self.per_node_metrics_list = per_node_metrics_list


class ExperimentPerNodeMetrics:
    node_name = None
    total_power_consumed = None
    power_consumed_in_stages = []
    total_disk_breads = None
    total_disk_bwrites = None

    def __init__(self, node_name, total_power_consumed, power_consumed_in_stages,
                 total_disk_breads = None, total_disk_bwrites = None):
        self.node_name = node_name
        self.total_power_consumed = total_power_consumed
        self.power_consumed_in_stages = power_consumed_in_stages
        self.total_disk_breads = total_disk_breads
        self.total_disk_bwrites = total_disk_bwrites

    # Calculates and returns ratios of power consumed in stages instead of absolute value for 'power_consumed_in_stages'
    def power_consumed_in_stages_ratios(self):
        sum_stage_power = sum(self.power_consumed_in_stages.values(), 0.0)
        return {k: v * 100 / sum_stage_power for k, v in self.power_consumed_in_stages.items()}


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

    # Get power file and parse it
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

    # Calculate total power consumed by each node, (in each spark stage) and add details to all_results.
    per_node_metrics_list = []
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

        result = ExperimentPerNodeMetrics(node_name, total_power_consumed, stages_power_list)
        per_node_metrics_list.append(result)

    return ExperimentMetrics(experiment_id, experiment_setup, per_node_metrics_list)


def plot_power_usages_across_experiments(run_id, experiment_metrics_list, output_dir):
    """
    Takes list of ExperimentMetrics objects, one for power usage on each node in each experiment
    """

    fig = plt.figure()
    fig.set_size_inches(w=7,h=20)
    fig.suptitle("Energy consumption on each node vs Link bandwidth")

    all_sizes = sorted(set(map(lambda r: r.input_size_gb, experiment_metrics_list)))
    count_sizes = all_sizes.__len__()
    counter = 0
    for size in all_sizes:
        counter += 1
        size_filtered = list(filter(lambda r: r.input_size_gb == size, experiment_metrics_list))
        all_nodes = set(map(lambda r: r.node_name, size_filtered))
        for node in all_nodes:
            node_filtered = list(filter(lambda r: r.node_name == node, size_filtered))

            # There may be multiple runs for the same setup, calculate average over those runs.
            all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, node_filtered)))
            avg_power_readings = []
            for link_rate in all_link_rates:
                filtered_power_readings = list(map(lambda m: m.total_power_consumed/3600,   # In watt-hours
                                                   filter(lambda f: f.link_bandwidth_mbps == link_rate, node_filtered)))
                avg_power_readings.append(np.mean(filtered_power_readings))

            ax = fig.add_subplot(count_sizes, 1, counter)
            ax.set_xlabel("Link bandwidth Mbps (Input size: {0}GB)".format(size))
            ax.set_ylabel("Energy (watt-hours)")
            ax.plot(all_link_rates, avg_power_readings, label=node, marker="x")

    plt.legend()
    plt.show()

    output_plot_file_name = "power_usage_kwh_{0}.png".format(run_id)
    output_full_path = os.path.join(output_dir, output_plot_file_name)
    # plt.savefig(output_full_path)


def plot_power_usages_one_node(run_id, experiment_power_results, output_dir, node_name='ccied22'):
    """
    Takes list of ExperimentRunResult objects, one for power usage on each node in each experiment
    """

    fig, ax = plt.subplots(1,1)
    fig.suptitle("Energy consumption on node {0} (Sort: No disk writes + hdfs caching)".format(node_name))

    experiment_power_results = list(filter(lambda r: r.node_name == node_name, experiment_power_results))
    all_sizes = sorted(set(map(lambda r: r.input_size_gb, experiment_power_results)))
    for size in all_sizes:
        size_filtered = list(filter(lambda r: r.input_size_gb == size, experiment_power_results))
        # There may be multiple runs for the same setup, calculate average over those runs.
        all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
        avg_power_readings = []
        for link_rate in all_link_rates:
            filtered_power_readings = list(map(lambda m: m.total_power_consumed/3600,   # In watt-hours
                                               filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered)))
            # avg_power_readings.append(math.log(np.mean(filtered_power_readings)))
            avg_power_readings.append(np.mean(filtered_power_readings))

        ax.set_xlabel("Link bandwidth Mbps (Input size: {0}GB)".format(size))
        ax.set_ylabel("Energy (watt-hours)")
        ax.plot(all_link_rates, avg_power_readings, label='{0} GB'.format(size), marker="x")

    plt.legend()
    plt.show()

    # output_plot_file_name = "power_usage_one_node_kwh_{0}.png".format(run_id)
    # output_full_path = os.path.join(output_dir, output_plot_file_name)
    # plt.savefig(output_full_path)


def plot_power_usage_in_stages_one_node(run_id, experiment_power_results, output_dir, node_name="ccied22"):
    """
    Takes list of ExperimentRunResult objects, one for power usage on each node in each experiment
    """

    fig = plt.figure()
    fig.set_size_inches(w=7,h=10)
    fig.suptitle("Energy consumption in each stage vs Link bandwidth")

    experiment_power_results = list(filter(lambda r: r.node_name == node_name, experiment_power_results))
    all_sizes = sorted(set(map(lambda r: r.input_size_gb, experiment_power_results)))
    all_sizes = [s for s in all_sizes if s >= 1]
    count_sizes = all_sizes.__len__()
    counter = 0
    for size in all_sizes:
        counter += 1
        size_filtered = list(filter(lambda r: r.input_size_gb == size, experiment_power_results))
        for stage in [0, 1, 2]:
            # There may be multiple runs for the same setup, calculate average over those runs.
            all_link_rates = sorted(set(map(lambda r: r.link_bandwidth_mbps, size_filtered)))
            avg_power_readings = []
            for link_rate in all_link_rates:
                filtered_power_readings = list(map(lambda m: m.power_consumed_in_stages[stage]/3600,  # In watt-hours
                                                   filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered)))
                filtered_power_readings_ratios = list(map(lambda m: m.power_consumed_in_stages_ratios()[stage],
                                               filter(lambda f: f.link_bandwidth_mbps == link_rate, size_filtered)))
                avg_power_readings.append(np.mean(filtered_power_readings))

            ax = fig.add_subplot(count_sizes, 1, counter)
            ax.set_xlabel("Link bandwidth Mbps (Input size: {0}GB)".format(size))
            ax.set_ylabel("Energy (watt-hours)")
            ax.plot(all_link_rates, avg_power_readings, label='Stage {0}'.format(stage))

    plt.legend()
    plt.show()

    output_plot_file_name = "power_usage_stage_kwh_{0}.png".format(run_id)
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
    plt.savefig(output_full_path)


def filter_experiments_to_consider():
    # 12/10 22:00 to 12/11 14:00 ---> No output + HDFS caching
    # 12/06 00:00 to 12/08 00:00 ---> No output
    # 11/29 00:00 to 12/01 00:00 ---> Legacy sort
    start_time = datetime.strptime('2018-12-13 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime('2018-12-16 00:00:00', '%Y-%m-%d %H:%M:%S')

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
    run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # Get experiments to consider
    experiments_to_consider = filter_experiments_to_consider()
    print(experiments_to_consider)

    all_results = [get_metrics_summary_for_experiment(exp_id) for exp_id in experiments_to_consider]
    # print(all_results)

    all_exps = set([p.experiment_id for p in all_results])
    power_readings = []
    for exp in all_exps:
        filter_exps = [p for p in all_results if p.experiment_id == exp]
        total_power = sum([p.total_power_consumed for p in filter_exps])
        power_readings.append(total_power/3600)
        print(total_power/3600)
    print(np.mean(power_readings), np.std(power_readings), np.max(power_readings) - np.min(power_readings))

    # plot_power_usages_across_experiments(run_id, all_results, power_plots_output_dir)
    # plot_power_usages_one_node(run_id, all_results, power_plots_output_dir)
    # plot_experiment_duration(run_id, all_results, power_plots_output_dir)
    # plot_power_usage_in_stages_one_node(run_id, all_results, power_plots_output_dir)

