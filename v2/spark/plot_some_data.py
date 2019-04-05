import os
import re
from datetime import datetime
from datetime import timedelta
import random
import matplotlib.pyplot as plt
from collections import Counter
import plot_one_experiment


line_regex = r'^\s+0-39\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+([0-9]+[\.]?[0-9]+)\s+[^\s]+$'


def main():
    experiment_id = "Exp-2019-02-19-10-02-33"
    experiment_dir_path = os.path.join(plot_one_experiment.results_base_dir, experiment_id)
    mem_bw_file_path = os.path.join(experiment_dir_path, "mb_output")
    values = Counter()
    i = 0
    with open(mem_bw_file_path, "r") as lines:
        for line in lines:
            matches = re.match(line_regex, line)
            if matches:
                value = float(matches.group(1))
                values[i] = value
                i += 1
    
    # fig, ax = plt.subplots(1, 1)
    # fig.suptitle("Local Mem Bandwidth ")
    # ax.set_xlabel("Seconds")
    # ax.set_ylabel("MB/s")
    # ax.plot(values.keys(), values.values())
    # plt.show()

    fig, ax = plt.subplots(1, 1)
    fig.suptitle("Mem access rate for Spark Sort 100GB")
    ax.set_xlabel("Mem access rate MB/s")
    ax.set_ylabel("CDF")
    readings = list(values.values())
    readings = [r for r in readings if r > 10]
    x, y = plot_one_experiment.gen_cdf(readings, 1000)
    ax.plot(x, y)
    plt.show()


if __name__ == '__main__':
    main()