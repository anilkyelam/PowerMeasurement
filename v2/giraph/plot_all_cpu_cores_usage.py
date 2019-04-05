"""
Plots cpu usage of all cores from a SAR-captured CPU usage file
"""

import os
import re
from datetime import datetime
from datetime import timedelta
import random
import matplotlib.pyplot as plt
import json
import traceback as tc
import plot_one_experiment


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
cpu_any_core_regex = r'^([0-9]+:[0-9]+:[0-9]+ [AP]M)\s+([a-z0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)$'


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
def parse_plot_cpu(experiment_name, node_name):

    all_readings = []
    experiment_folder_path = os.path.join(plot_one_experiment.results_base_dir, experiment_name)
    cpu_file_path = os.path.join(experiment_folder_path, node_name, "cpu.sar")
    with open(cpu_file_path, "r") as lines:
        first_line = True
        date_part = None
        previous_reading_time_part = None
        for line in lines:
            if first_line:
                date_string = parse_date_from_sar_file(first_line_in_file=line)
                date_part = datetime.strptime(date_string, '%m/%d/%Y')
                first_line = False

            matches = re.match(cpu_any_core_regex, line)
            if matches:
                time_string = matches.group(1)

                # Add a day when experiment runs past midnight, when the hour of the first reading is smaller than the one before.
                time_part = datetime.strptime(time_string, '%I:%M:%S %p')
                if previous_reading_time_part is not None and previous_reading_time_part.hour > time_part.hour:
                    date_part = date_part + timedelta(days=1)
                previous_reading_time_part = time_part

                timestamp = date_part.replace(hour=time_part.hour, minute=time_part.minute, second=time_part.second)
                cpu_id = "cpu_" + matches.group(2)
                cpu_user_usage = float(matches.group(3))
                cpu_system_usage = float(matches.group(5))

                all_readings.append([timestamp, cpu_id, "cpu_user_usage", cpu_user_usage])
                all_readings.append([timestamp, cpu_id, "cpu_system_usage", cpu_system_usage])
                all_readings.append([timestamp, cpu_id, "cpu_total_usage", cpu_user_usage + cpu_system_usage])

    fig, ax = plt.subplots(1, 1)
    fig.set_size_inches(w=15,h=7)
    fig.suptitle("All Cores CPU Usage (for some short interval)")
    ax.set_xlabel("Time")
    ax.set_ylabel("CPU %")

    # Time filter readings: Take a random 1 min interval i.e., 60
    start_time = min([r[0] for r in all_readings])
    end_time = max([r[0] for r in all_readings])
    filter_start_time = start_time + timedelta(minutes=1)
    filter_end_time = start_time + timedelta(minutes=2)
    all_readings = [r for r in all_readings if filter_start_time < r[0] < filter_end_time]

    all_cpus = set([r[1] for r in all_readings])
    for cpu_id in all_cpus:
        filtered_readings = list(filter(lambda r: r[1] == cpu_id and r[2] == "cpu_total_usage", all_readings))
        ax.plot(list(map(lambda r: r[0], filtered_readings)), list(map(lambda r: r[3], filtered_readings)), label=cpu_id)

    # Save the file, should be done before show()
    plt.legend()
    output_plot_file_name = "plot_{0}.png".format("all_core_cpu_usage")
    output_full_path = os.path.join(experiment_folder_path, output_plot_file_name)
    plt.savefig(output_full_path)
    plt.show()
    plt.close()


if __name__ == "__main__": 
    # parse_plot_cpu("Exp-2019-03-26-20-59-28", "b09-32")           # First good 2b edges run
    # parse_plot_cpu("Exp-2019-03-29-23-48-48", "b09-34")           # 
    parse_plot_cpu("Exp-2019-04-01-22-50-25", "b09-34")             # 5b edges run, CPU limited at 60%
    