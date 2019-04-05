"""
Parses Power, CPU, Memory and other resource usage results and plots them for a single experiment
"""

import os
import re
from datetime import datetime
from datetime import timedelta
import random
import matplotlib.pyplot as plt
import json
import traceback as tc


# Experiment setup class
class ExperimentSetup:
    def __init__(self, setup_file_path):
        # Parse experimental setup from setup file
        json_dict = json.load(open(setup_file_path, "r"))
        self.hdfs_nodes = json_dict["HdfsNodes"]
        self.designated_driver_node = json_dict["GiraphDriverNode"]
        self.power_meter_nodes_in_order = json_dict["PowerMeterNodesInOrder"]
        self.input_graph_file = json_dict["InputGraphFile"]
        self.link_bandwidth_mbps = float(json_dict["LinkBandwidthMbps"])
        self.experiment_start_time = datetime.strptime(json_dict["ExperimentStartTime"], "%Y-%m-%d %H:%M:%S")
        self.job_start_time = datetime.strptime(json_dict["GiraphJobStartTime"], "%Y-%m-%d %H:%M:%S")
        self.job_end_time = datetime.strptime(json_dict["GiraphJobEndTime"], "%Y-%m-%d %H:%M:%S")
        
        self.experiment_group = str(json_dict["ExperimentGroup"])
        self.experiment_group_desc = json_dict["ExperimentGroupDesc"]
        self.giraph_class_name = json_dict["GiraphClassName"]
        self.input_cached_in_hdfs = json_dict["InputHdfsCached"] if "InputHdfsCached" in json_dict else None


# Results base folder
results_base_dir = "D:\Power Measurements\\v2\\giraph"
# results_base_dir = "D:\Power Measurements\\v2\Bad runs"
setup_details_file_name = "setup_details.txt"
cpu_readings_file_name = "cpu.sar"
mem_readings_file_name = "memory.sar"
net_readings_file_name = "network.sar"
diskio_readings_file_name = "diskio.sar"
power_readings_file_name = "power_readings.txt"
spark_log_file_name = 'spark.log'


# Regex patterns. https://regex101.com/
# First line of every SAR file to get date
first_line_regex = r'^Linux.+\s+([0-9]+[/-][0-9]+[/-][0-9]+)\s+'
# Example: <06:38:09 PM     all      3.78      0.00      2.52      0.50      0.00     93.20>
cpu_all_cores_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+(all)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)$'
# Example: <06:38:09 PM        lo     12.00     12.00      2.13      2.13      0.00      0.00      0.00      0.00>
network_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+([a-z0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+'
# Example: <06:38:09 PM    779700  15647244     95.25     65368   7407236  15733700     47.40   9560972   5486404      1292>
memory_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+'
# Example: <06:38:09 PM     21.00     21.00      0.00   2416.00      0.00>
io_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)\s+([0-9]+[\.]?[0-9]+)$'
# Example: <1541731089.0383,112.50,95.172,98.975,97.549>
power_regex = r'^([0-9]+[\.]?[0-9]+),([0-9]+[\.]?[0-9]+),([0-9]+[\.]?[0-9]+),([0-9]+[\.]?[0-9]+),([0-9]+[\.]?[0-9]+)'


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


# Collects all results from SAR and Powermeter, parses for required info and merges results onto single timeline.
def parse_results(results_dir_path, experiment_setup, output_readings_file_name, output_readings_to_file=False):
    # Final results
    all_readings = []

    # Parse SAR readings for each node
    for node_name in experiment_setup.hdfs_nodes:
        node_results_dir = os.path.join(results_dir_path, node_name)

        # Parse CPU results
        cpu_full_path = os.path.join(node_results_dir, cpu_readings_file_name)

        with open(cpu_full_path, "r") as lines:
            first_line = True
            date_part = None
            previous_reading_time_part = None
            for line in lines:
                if first_line:
                    date_string = parse_date_from_sar_file(first_line_in_file=line)
                    date_part = datetime.strptime(date_string, '%m/%d/%Y')
                    first_line = False

                matches = re.match(cpu_all_cores_regex, line)
                if matches:
                    time_string = matches.group(1)

                    # Add a day when experiment runs past midnight, when the hour of the first reading is smaller than the one before.
                    time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                    if previous_reading_time_part is not None and previous_reading_time_part.hour > time_part.hour:
                        date_part = date_part + timedelta(days=1)
                    previous_reading_time_part = time_part

                    timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                    cpu_user_usage = float(matches.group(3))
                    cpu_system_usage = float(matches.group(5))

                    all_readings.append([timestamp, node_name, "cpu_user_usage", cpu_user_usage])
                    all_readings.append([timestamp, node_name, "cpu_system_usage", cpu_system_usage])
                    all_readings.append([timestamp, node_name, "cpu_total_usage", cpu_user_usage + cpu_system_usage])

        # Parse network usage
        net_full_path = os.path.join(node_results_dir, net_readings_file_name)

        with open(net_full_path, "r") as lines:
            first_line = True
            date_part = None
            previous_reading_time_part = None
            for line in lines:
                if first_line:
                    date_string = parse_date_from_sar_file(first_line_in_file=line)
                    date_part = datetime.strptime(date_string, '%m/%d/%Y')
                    first_line = False

                matches = re.match(network_regex, line)
                if matches:
                    time_string = matches.group(1)

                    # Add a day when experiment runs past midnight, when the hour of the first reading is smaller than the one before.
                    time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                    if previous_reading_time_part is not None and previous_reading_time_part.hour > time_part.hour:
                        date_part = date_part + timedelta(days=1)
                    previous_reading_time_part = time_part

                    timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                    net_interface = matches.group(2)
                    net_in_KBps = float(matches.group(5))
                    net_out_KBps = float(matches.group(6))

                    # Taking only eth0 interface for now.
                    if net_interface == "enp101s0":
                        all_readings.append([timestamp, node_name, "net_in_Mbps", net_in_KBps * 8 / 1000])
                        all_readings.append([timestamp, node_name, "net_out_Mbps", net_out_KBps * 8 / 1000])
                        all_readings.append([timestamp, node_name, "net_total_Mbps", (net_in_KBps + net_out_KBps) * 8/ 1000])

        # Parse memory usage
        mem_full_path = os.path.join(node_results_dir, mem_readings_file_name)

        with open(mem_full_path, "r") as lines:
            first_line = True
            date_part = None
            previous_reading_time_part = None
            for line in lines:
                if first_line:
                    date_string = parse_date_from_sar_file(first_line_in_file=line)
                    date_part = datetime.strptime(date_string, '%m/%d/%Y')
                    first_line = False

                matches = re.match(memory_regex, line)
                if matches:
                    time_string = matches.group(1)

                    # Add a day when experiment runs past midnight, when the hour of the first reading is smaller than the one before.
                    time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                    if previous_reading_time_part is not None and previous_reading_time_part.hour > time_part.hour:
                        date_part = date_part + timedelta(days=1)
                    previous_reading_time_part = time_part

                    timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                    mem_usage_percent = float(matches.group(5))

                    all_readings.append([timestamp, node_name, "mem_usage_percent", mem_usage_percent])

        # Parse disk IO usage (Disk IO may not exist for some experiments, so skip it if it does not exist)
        diskio_full_path = os.path.join(node_results_dir, diskio_readings_file_name)
        if os.path.exists(diskio_full_path):
            with open(diskio_full_path, "r") as lines:
                first_line = True
                date_part = None
                previous_reading_time_part = None
                for line in lines:
                    if first_line:
                        date_string = parse_date_from_sar_file(first_line_in_file=line)
                        date_part = datetime.strptime(date_string, '%m/%d/%Y')
                        first_line = False

                    matches = re.match(io_regex, line)
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

                        all_readings.append([timestamp, node_name, "disk_reads_ps", disk_rps])
                        all_readings.append([timestamp, node_name, "disk_writes_ps", disk_wps])
                        all_readings.append([timestamp, node_name, "disk_total_ps", disk_rps + disk_wps])
                        all_readings.append([timestamp, node_name, "disk_breads_ps", disk_brps])
                        all_readings.append([timestamp, node_name, "disk_bwrites_ps", disk_bwps])
                        all_readings.append([timestamp, node_name, "disk_btotal_ps", disk_brps + disk_bwps])
                        all_readings.append([timestamp, node_name, "disk_MBreads_ps", disk_brps * 512 / (1024 * 1024)])
                        all_readings.append([timestamp, node_name, "disk_MBwrites_ps", disk_bwps * 512 / (1024 * 1024)])
                        all_readings.append([timestamp, node_name, "disk_MBtotal_ps", (disk_brps + disk_bwps) * 512 / (1024 * 1024)])

    # Parse power measurements and attribute them to each node connected to power meter
    designated_driver_results_path = os.path.join(results_dir_path, experiment_setup.designated_driver_node)
    power_full_path = os.path.join(designated_driver_results_path, power_readings_file_name)

    if os.path.exists(power_full_path):
        with open(power_full_path, "r") as lines:
            for line in lines:
                matches = re.match(power_regex, line)
                if matches:
                    timestamp = datetime.fromtimestamp(float(matches.group(1)))

                    i = 0
                    for node_name in experiment_setup.power_meter_nodes_in_order:
                        power_watts = float(matches.group(i + 2))
                        all_readings.append([timestamp.replace(microsecond=0), node_name, "power_watts", power_watts])
                        i += 1

    # Parse giraph log

    # Output to file
    if output_readings_to_file:
        output_full_path = os.path.join(results_dir_path, output_readings_file_name)

        with open(output_full_path, "w") as f:
            for r in all_readings:
                f.write("{0}, {1}, {2}\n".format(r[0], r[1], r[2]))

    return all_readings


# Generates one plot for resource usages per node
def plot_all_for_one_node(plots_dir_full_path, all_readings, experiment_id, experiment_setup, node_name):

    # Filter all readings for node
    all_readings = list(filter(lambda r: r[1] == node_name, all_readings))

    fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1)
    fig.set_size_inches(w=10,h=10)
    fig.suptitle("Experiment ID: {0}\nPage rank on graph:{1}, Link bandwidth: {2}Mbps, Role: {3}".format(
        experiment_id, experiment_setup.input_graph_file, experiment_setup.link_bandwidth_mbps,
        "Driver" if node_name == experiment_setup.designated_driver_node else "Executor"))

    # render subplots
    render_subplot_by_label(ax1, all_readings,
                   filter_label='power_watts',
                   x_label='',
                   y_label='Power (units?)')
    render_subplot_by_label(ax2, all_readings,
                   filter_label='cpu_total_usage',
                   x_label='',
                   y_label='CPU usage % (all cores)')
    render_subplot_by_label(ax3, all_readings,
                   filter_label='mem_usage_percent',
                   x_label='',
                   y_label='Memory used %')
    render_subplot_by_label(ax4, all_readings,
                   filter_label='net_in_Mbps',
                   x_label='',
                   y_label='Network Mbps',
                   plot_label='In')
    render_subplot_by_label(ax4, all_readings,
                   filter_label='net_out_Mbps',
                   x_label='',
                   y_label='Network Mbps',
                   plot_label='Out')
    render_subplot_by_label(ax5, all_readings,
                   filter_label='disk_MBreads_ps',
                   x_label='',
                   y_label='Disk MB/sec',
                   plot_label='Reads')
    render_subplot_by_label(ax5, all_readings,
                   filter_label='disk_MBwrites_ps',
                   x_label='Time (in secs)',
                   y_label='Disk MB/sec',
                   plot_label='Writes')

    # Save the file, should be done before show()
    output_plot_file_name = "plot_{0}_{1}.png".format(experiment_setup.input_graph_file, node_name)
    output_full_path = os.path.join(plots_dir_full_path, output_plot_file_name)
    plt.savefig(output_full_path)
    # plt.show()
    plt.close()


# Generates one plot for resource usages on one node
def plot_all_for_one_label(plots_dir_full_path, all_readings, experiment_id, experiment_setup, label_name):

    # Filter all readings for node
    all_readings = list(filter(lambda r: r[2] == label_name, all_readings))

    fig, ax = plt.subplots(1, 1)
    fig.set_size_inches(w=20,h=10)
    fig.suptitle("Experiment ID: {0}\nPageRank on graph:{1}, Link bandwidth: {2}Mbps, Label: {3}".format(
        experiment_id, experiment_setup.input_graph_file, experiment_setup.link_bandwidth_mbps, label_name))

    # render subplots
    for node_name in experiment_setup.hdfs_nodes:
        render_subplot_by_node(ax, all_readings,
                        filter_node=node_name,
                        x_label='',
                        y_label=label_name,
                        plot_label=node_name)

    # Save the file, should be done before show()
    output_plot_file_name = "plot_{0}_{1}.png".format(experiment_setup.input_graph_file, label_name)
    output_full_path = os.path.join(plots_dir_full_path, output_plot_file_name)
    plt.savefig(output_full_path)
    # plt.show()
    plt.close()


# Filters a subset of readings from all readings based on the filter label and plots it on provided axes
def render_subplot_by_label(ax, all_readings, filter_label, x_label, y_label, plot_label=None):
    filtered_readings = list(filter(lambda r: r[2] == filter_label, all_readings))
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    if plot_label is not None:
        ax.plot(list(map(lambda r: r[0], filtered_readings)), list(map(lambda r: r[3], filtered_readings)), label=plot_label)
        ax.legend()
    else:
        ax.plot(list(map(lambda r: r[0], filtered_readings)), list(map(lambda r: r[3], filtered_readings)))


# Filters a subset of readings from all readings based on the filter label and plots it on provided axes
def render_subplot_by_node(ax, all_readings, filter_node, x_label, y_label, plot_label=None):
    filtered_readings = list(filter(lambda r: r[1] == filter_node, all_readings))
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    if plot_label is not None:
        ax.plot(list(map(lambda r: r[0], filtered_readings)), list(map(lambda r: r[3], filtered_readings)), label=plot_label)
        ax.legend()
    else:
        ax.plot(list(map(lambda r: r[0], filtered_readings)), list(map(lambda r: r[3], filtered_readings)))


# Parse results and generate plots for one experiment, and returns full path to the output folder
def parse_and_plot_results(experiment_id):
    results_dir_name = experiment_id
    results_dir_path = os.path.join(results_base_dir, results_dir_name)
    setup_file_path = os.path.join(results_dir_path, setup_details_file_name)
    experiment_setup = ExperimentSetup(setup_file_path)

    # Assign a random run id for this parse run and use it for generated outputs
    random_parse_run_id = random.randint(1, 1001)
    plots_dir_name = "plots_{0}".format(random_parse_run_id)
    output_readings_file_name = "all_readings_{0}.txt".format(random_parse_run_id)

    # Collect readings from all results files
    all_readings = parse_results(results_dir_path, experiment_setup, output_readings_file_name,
                                 output_readings_to_file=False)

    # Generate plots for each node
    plots_dir_full_path = os.path.join(results_dir_path, plots_dir_name)
    if not os.path.exists(plots_dir_full_path):
        os.mkdir(plots_dir_full_path)

    for node_name in experiment_setup.hdfs_nodes:
        plot_all_for_one_node(plots_dir_full_path, all_readings, experiment_id, experiment_setup, node_name)

    for label_name in ('power_watts', 'cpu_total_usage', 'mem_usage_percent', 'net_in_Mbps', 'net_out_Mbps',
                       'disk_MBreads_ps', 'disk_MBwrites_ps'):
        plot_all_for_one_label(plots_dir_full_path, all_readings, experiment_id, experiment_setup, label_name)
    
    return plots_dir_full_path


# Filter experiments to generate plots
def filter_experiments_to_consider():
    start_time = datetime.strptime('2019-03-10 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.now()

    # All experiments after start_time that doesn't already have plots_ folder.
    experiments_to_consider = []
    all_experiments = [os.path.join(results_base_dir, item) for item in os.listdir(results_base_dir)
               if item.startswith("Exp-")
                   and os.path.isdir(os.path.join(results_base_dir, item))
                   and not [subdir for subdir in os.listdir(os.path.join(results_base_dir, item)) if subdir.startswith("plots_")]]

    for experiment_dir_path in all_experiments:
        experiment_id = os.path.basename(experiment_dir_path)
        experiment_time = datetime.fromtimestamp(os.path.getctime(experiment_dir_path))
        if start_time < experiment_time < end_time:
            experiments_to_consider.append(experiment_id)
            pass

    # experiments_to_consider.append("Exp-2019-01-30-18-21-58")

    return experiments_to_consider


if __name__ == "__main__":
    # all_experiments = ["Sting-Exp-2018-12-19-23-43-24"]
    all_experiments = filter_experiments_to_consider()
    for experiment_id in all_experiments:
        try:
            print("Parsing experiment " + experiment_id)
            parse_and_plot_results(experiment_id)
        except Exception as e:
            tc.print_exc(e)
            pass
