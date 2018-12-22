import os
import re
from datetime import datetime
from dateutil import tz
from collections import Counter
import plot_one_experiment

# Constants
power_meter_nodes_in_order = ["ccied21", "ccied22", "ccied23", "ccied24"]

def parse_power_usage_across_experiments():
    # 2018-12-13 13:54:28	2018-12-13 14:50:28

    utc_zone = tz.gettz('UTC')
    pst_zone = tz.gettz('PST')

    exp_start_time_utc = datetime.strptime("2018-12-20 00:11:10", "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_zone)
    exp_end_time_utc = datetime.strptime("2018-12-20 01:06:06", "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_zone)

    # exp_start_time = exp_start_time_utc.astimezone(pst_zone).replace(tzinfo=None)
    # exp_end_time = exp_end_time_utc.astimezone(pst_zone).replace(tzinfo=None)
    # exp_start_time = datetime.strptime("18/12/09 16:25:01", "%y/%m/%d %H:%M:%S")
    # exp_end_time = datetime.strptime("18/12/09 17:24:03", "%y/%m/%d %H:%M:%S")
    exp_start_time = datetime.strptime("2018-12-20 20:52:23", "%Y-%m-%d %H:%M:%S")
    exp_end_time = datetime.strptime("2018-12-20 21:50:28", "%Y-%m-%d %H:%M:%S")

    # Get power file and parse it
    raw_power_readings = Counter()
    all_power_readings = []
    power_readings_file_name = "power_1545367936_1000.txt"
    power_readings_file_path = os.path.join("D:\Git\PowerMeasurement\\temp", power_readings_file_name)
    # power_readings_file_path = os.path.join("D:\Power Measurements\Exp-2018-12-10-18-12-42\ccied21", "power_readings.txt")
    power_readings_counter = 0
    with open(power_readings_file_path, "r") as lines:
        for line in lines:
            matches = re.match(plot_one_experiment.power_regex, line)
            if matches:
                timestamp = datetime.fromtimestamp(float(matches.group(1)))
                # print(timestamp, power_readings_counter)
                power_readings_counter += 1

                i = 0
                for node_name in power_meter_nodes_in_order:
                    power_watts = float(matches.group(i + 2))
                    all_power_readings.append([timestamp.replace(microsecond=0), node_name, "power_watts", power_watts])
                    raw_power_readings[timestamp] += power_watts
                    i += 1

    # Simple sanity check: Alert if total number of power readings does not exceed experiment duration
    if power_readings_counter + 10 < (exp_end_time - exp_start_time).seconds:
        raise Exception("Number of power readings does not match experiment duration for Experiment!")

    # Filter power readings outside of the spark job time range
    all_power_readings = list(filter(lambda r: r[0] < exp_end_time, all_power_readings))

    # Calculate total power consumed by each node, (in each spark stage) and add details to all_results.
    total_power_consumed = 0
    for node_name in power_meter_nodes_in_order:
        all_readings_node = list(filter(lambda r: r[1] == node_name, all_power_readings))
        power_consumed = sum(map(lambda r: r[3], all_readings_node))
        total_power_consumed += power_consumed

    print("Experiment: {0}, Times {0} to {1}".format(power_readings_file_name, exp_start_time, exp_end_time))
    print("Total power joules: " + str(total_power_consumed))
    print("Total power watt-hours: " + str(total_power_consumed/3600))

    # Raw results analysis
    min_time = min(raw_power_readings.keys())
    max_time = max(raw_power_readings.keys())
    print(min_time, max_time)

    total_raw_power = sum(raw_power_readings.values())
    raw_power_during_exp = sum([value for key, value in raw_power_readings.items() if key < exp_end_time])

    print("Power collection times {0} to {1}".format(min_time, max_time))
    print("Raw total power {0} watt-hours and duration {1} seconds".format(total_raw_power/3600, (max_time - min_time).seconds))
    print("Raw exp power {0} watt-hours and duration {1} minutes".format(round(raw_power_during_exp/3600, 2),
                                                                         round((exp_end_time - exp_start_time).seconds/60, 1)))


if __name__ == '__main__':
    parse_power_usage_across_experiments()
