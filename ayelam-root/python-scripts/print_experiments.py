"""
Filters experiments from the results folder based on some setup parameters
"""


import os
from sys import stdin
from datetime import datetime
import plot_one_experiment
from plot_one_experiment import ExperimentSetup


def main():
    results_dir = plot_one_experiment.results_base_dir
    all_experiments = [os.path.join(results_dir, item) for item in os.listdir(results_dir)
               if item.startswith("Exp-") and os.path.isdir(os.path.join(results_dir, item))]
    experiments_to_consider = list(filter(
        lambda path: datetime.fromtimestamp(os.path.getctime(path)) > datetime(2018, 12, 10),
        all_experiments))

    filter_results = False
    sizes_filter = [10]
    link_bw_filter = [400]
    for experiment_dir_path in experiments_to_consider:
        experiment_id = os.path.basename(experiment_dir_path)
        # print("Parsing experiment {0}".format(experiment_id))

        # Read setup file to get experiment parameters
        setup_file_path = os.path.join(experiment_dir_path, "setup_details.txt")
        experiment_setup = ExperimentSetup(setup_file_path)

        if not filter_results or \
            (experiment_setup.input_size_gb in sizes_filter and experiment_setup.link_bandwidth_mbps in link_bw_filter):
            print(experiment_id, experiment_setup.input_size_gb, experiment_setup.link_bandwidth_mbps,
                  experiment_setup.input_cached_in_hdfs)

    stdin.read()


if __name__ == '__main__':
    main()
