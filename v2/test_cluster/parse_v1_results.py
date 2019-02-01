"""
Parses raw result files from v1 experiments and aggregates power measurements
"""

import os
import re
from datetime import datetime
from dateutil import tz
from collections import Counter
import plot_one_experiment

# Constants
power_meter_nodes_in_order = ["ccied21", "ccied22", "ccied23", "ccied24"]


def get_spark_run_times(spark_log_file_path):
    # Read stages and other timestamps from spark log file
    all_timestamp_list = []
    stages_time_stamp_list = []
    with open(spark_log_file_path, "r") as lines:
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
    return min(all_timestamp_list), max(all_timestamp_list)


def get_power_usage_wh(power_log_file_path, exp_start_time, exp_end_time):
    # Get power file and parse it
    raw_power_readings = Counter()
    power_readings_counter = 0
    with open(power_log_file_path, "r") as lines:
        for line in lines:
            matches = re.match(plot_one_experiment.power_regex, line)
            if matches:
                timestamp = datetime.fromtimestamp(float(matches.group(1)))
                power_readings_counter += 1

                i = 0
                for _ in power_meter_nodes_in_order:
                    power_watts = float(matches.group(i + 2))
                    raw_power_readings[timestamp] += power_watts
                    i += 1

    # Simple sanity check: Alert if total number of power readings does not exceed experiment duration
    if power_readings_counter + 10 < (exp_end_time - exp_start_time).seconds:
        # raise Exception("Number of power readings does not match experiment duration for Experiment!")
        print("Number of power readings does not match experiment duration for Experiment! : " + str(power_readings_counter))

    raw_power_during_exp = sum([value for key, value in raw_power_readings.items() if key < exp_end_time])
    return raw_power_during_exp/3600, min(raw_power_readings.keys()), max(raw_power_readings.keys())


def main():
    base_dir = "D:\Git\PowerMeasurement\\temp\\anil_runs_1_11"
    # base_dir = 'D:\Git\PowerMeasurement\\temp\sting_runs_1_9_500'
    # base_dir = "D:\Git\PowerMeasurement\\temp\sting_runs_12_20"

    # spark_log_file_name = "spark_128gb_1545983908_1000.log"
    # power_readings_file_name = "power_1545983908_1000.txt"

    last_exp_start_time = datetime.min
    for (_, _, filenames) in os.walk(base_dir):
        for file_name in filenames:
            if file_name.startswith("spark_128gb_"):
                # print(file_name)
                spark_log_file_name = file_name
                exp_id = re.match(r"spark_.*_([0-9]+)_.+", file_name).group(1)
                link_mbps = re.match(r"spark_.*_[0-9]+_([0-9]+).+", file_name).group(1)
                power_readings_file_name = "power_{0}_{1}.txt".format(exp_id, link_mbps)
                if os.path.exists(os.path.join(base_dir, power_readings_file_name)):
                    # print(spark_log_file_name, power_readings_file_name)

                    start_time, end_time = get_spark_run_times(os.path.join(base_dir, spark_log_file_name))
                    power_value, power_start, power_end = get_power_usage_wh(os.path.join(base_dir, power_readings_file_name), start_time, end_time)
                    # hours_since_baseline = (start_time - datetime(2018, 12, 27)).total_seconds()/3600
                    spark_duration = round((end_time - start_time).seconds/60, 2)
                    power_log_duration = round((power_end - power_start).seconds/60, 2)

                    # Divide sets of exp runs based on exp time
                    if (start_time - last_exp_start_time).total_seconds() > 7200:
                        print("=======================================================")
                    last_exp_start_time = start_time

                    # print(exp_id, link_mbps, start_time, end_time, spark_duration, power_log_duration, round(power_value, 2), sep = ", ")
                    print(exp_id, link_mbps, start_time, end_time, spark_duration, power_log_duration, round(power_value, 2), sep = ", ")
                    # print(round(power_value, 2))


    # parse_power_usage_across_experiments()


if __name__ == '__main__':
    main()
