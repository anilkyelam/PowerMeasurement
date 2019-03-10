'''
A fun experiment to see Power vs CPU, Memory, Network & Disk correlation
since we have these readings from hundreds of runs on ccied machines
'''

import os
import random
from datetime import datetime 
from datetime import timedelta
import plot_one_experiment
from plot_one_experiment import results_base_dir, parse_results, ExperimentSetup, setup_details_file_name
import json


# Constants
power_entries_file_name = "power_vs_resource_usage_entries"
scratch_dir = 'D:\Power Measurements\\v1\PowerCorrelation'


class PowerEntry:
    timestamp = None
    node_name = None
    power_watts = None
    cpu_total_usage = None
    mem_usage_percent = None
    net_in_KBps = None
    net_out_KBps = None
    net_total_KBps = None
    disk_breads_ps = None
    disk_bwrites_ps = None
    disk_btotal_ps = None

    def __init__(self, json_dict= None):
        if json_dict is None:
            return
        self.timestamp = datetime.strptime(json_dict["timestamp"], "%Y-%m-%d %H:%M:%S")
        self.node_name = json_dict["node_name"]
        self.power_watts = json_dict["power_watts"]
        self.cpu_total_usage = json_dict["cpu_total_usage"]
        self.mem_usage_percent = json_dict["mem_usage_percent"]
        self.net_in_KBps = json_dict["net_in_KBps"]
        self.net_out_KBps = json_dict["net_out_KBps"]
        self.net_total_KBps = json_dict["net_total_KBps"]
        self.disk_breads_ps = json_dict["disk_breads_ps"]
        self.disk_bwrites_ps = json_dict["disk_bwrites_ps"]
        self.disk_btotal_ps = json_dict["disk_btotal_ps"]

    def to_json_dict(self):
        return {
            "timestamp" : self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "node_name" : self.node_name,
            "power_watts" : self.power_watts,
            "cpu_total_usage" : self.cpu_total_usage,
            "mem_usage_percent" : self.mem_usage_percent,
            "net_in_KBps" : self.net_in_KBps,
            "net_out_KBps" : self.net_out_KBps,
            "net_total_KBps" : self.net_total_KBps,
            "disk_breads_ps" : self.disk_breads_ps,
            "disk_bwrites_ps" : self.disk_bwrites_ps,
            "disk_btotal_ps" : self.disk_bwrites_ps,
        }


# For each experiment, parses the power, cpu, disk, etc files, correlated the readings based on timestamp and saves them 
def parse_and_store_power_entries():
    start_time = datetime.strptime('2018-11-24 00:00:00', '%Y-%m-%d %H:%M:%S')
    all_experiments = [os.path.join(results_base_dir, item) for item in os.listdir(results_base_dir)
               if item.startswith("Exp-")
                   and os.path.isdir(os.path.join(results_base_dir, item))
                   and not [subdir for subdir in os.listdir(os.path.join(results_base_dir, item)) if subdir.startswith("plots_")]]


    for experiment_dir_path in all_experiments:
        
        # From when we have nicely formatted and parse-able readings
        experiment_time = datetime.fromtimestamp(os.path.getctime(experiment_dir_path))
        if experiment_time < start_time:
            continue

        # If the parsing is already done for this experiment, skip it
        power_entries_file_path = os.path.join(experiment_dir_path, power_entries_file_name)
        if os.path.exists(power_entries_file_path):
            continue

        experiment_id = os.path.basename(experiment_dir_path)
        print("Parsing results of experiment: " + experiment_id)
        
        results_dir_path = os.path.join(results_base_dir, experiment_id)
        setup_file_path = os.path.join(results_dir_path, setup_details_file_name)
        experiment_setup = ExperimentSetup(setup_file_path)

        # Collect readings from all results files
        power_entries = []
        all_readings = parse_results(results_dir_path, experiment_setup, None, output_readings_to_file=False)
        max_timestamp = max([r[0] for r in all_readings])
        min_timestamp = min([r[0] for r in all_readings])
        timestamp = min_timestamp
        while timestamp < max_timestamp:
            for node_name in ["ccied21", "ccied22", "ccied23", "ccied24"]:
                power_entry = PowerEntry()
                power_entry.timestamp = timestamp
                power_entry.node_name = node_name

                readings = [r for r in all_readings if r[0] == timestamp and r[1] == node_name]
                for r in readings:
                    if r[2] == "power_watts":           power_entry.power_watts = r[3]
                    if r[2] == "cpu_total_usage":       power_entry.cpu_total_usage = r[3]
                    if r[2] == "mem_usage_percent":     power_entry.mem_usage_percent = r[3]
                    if r[2] == "net_in_KBps":           power_entry.net_in_KBps = r[3]
                    if r[2] == "net_out_KBps":          power_entry.net_out_KBps = r[3]
                    if r[2] == "net_total_KBps":        power_entry.net_total_KBps = r[3]
                    if r[2] == "disk_breads_ps":        power_entry.disk_breads_ps = r[3]
                    if r[2] == "disk_bwrites_ps":       power_entry.disk_bwrites_ps = r[3]
                    if r[2] == "disk_btotal_ps":        power_entry.disk_btotal_ps = r[3]

                power_entries.append(power_entry)
            timestamp += timedelta(seconds=1)
        
        power_entries_file = open(power_entries_file_path, "w")
        # print([p.to_json_dict() for p in power_entries])
        json.dump([p.to_json_dict() for p in power_entries], power_entries_file, indent=4)



# Reads in the power entries stored in a file for each experiment
def read_power_entries():
    start_time = datetime.strptime('2018-11-24 00:00:00', '%Y-%m-%d %H:%M:%S')
    all_experiments = [os.path.join(results_base_dir, item) for item in os.listdir(results_base_dir)
               if item.startswith("Exp-")
                   and os.path.isdir(os.path.join(results_base_dir, item))
                   and not [subdir for subdir in os.listdir(os.path.join(results_base_dir, item)) if subdir.startswith("plots_")]]

    all_power_entries = []
    for experiment_dir_path in all_experiments[0:10]:
        # If the parsing is done, there will be a power entries file
        power_entries_file_path = os.path.join(experiment_dir_path, power_entries_file_name)
        if not os.path.exists(power_entries_file_path):
            continue

        power_entries_file = open(power_entries_file_path, "r")
        power_entries_dicts = json.load(power_entries_file)
        power_entries = [PowerEntry(p) for p in power_entries_dicts]
        all_power_entries += power_entries

    output_file_path = os.path.join(scratch_dir, "power_vs_all_ccied21.txt")
    with open(output_file_path, 'a') as out_file:
        for p in all_power_entries:
            if p.node_name == "ccied21" and p.power_watts is not None \
                and p.mem_usage_percent is not None \
                and p.cpu_total_usage is not None \
                and p.disk_btotal_ps is not None \
                and p.net_total_KBps is not None:
                out_file.write("{0}, {1}, {2}, {3}, {4} \n".format(p.power_watts, p.cpu_total_usage, p.mem_usage_percent,
                    p.net_total_KBps, p.disk_btotal_ps))


if __name__ == "__main__":
    # parse_and_store_power_entries()
    read_power_entries()    