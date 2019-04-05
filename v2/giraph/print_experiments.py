"""
Filters experiments from the results folder based on some setup parameters
"""


import os
from sys import stdin
from datetime import datetime
import plot_one_experiment
from plot_one_experiment import ExperimentSetup


# Filter experiments to generate plots
def filter_experiments_to_consider():
    start_time = datetime.strptime('2019-03-15 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.now()

    # All experiments after start_time that doesn't already have plots_ folder.
    results_base_dir = plot_one_experiment.results_base_dir
    experiments_to_consider = []
    all_experiments = [os.path.join(results_base_dir, item) for item in os.listdir(results_base_dir)
               if item.startswith("Exp-")
                   and os.path.isdir(os.path.join(results_base_dir, item))]

    for experiment_dir_path in all_experiments:
        experiment_id = os.path.basename(experiment_dir_path)
        experiment_time = datetime.fromtimestamp(os.path.getctime(experiment_dir_path))
        if start_time < experiment_time < end_time:
            experiments_to_consider.append(experiment_id)
            pass

    # experiments_to_consider.append("Exp-2019-01-30-18-21-58")

    return experiments_to_consider


def main():
    results_dir = plot_one_experiment.results_base_dir
    all_experiments = filter_experiments_to_consider()

    filter_results = False
    sizes_filter = [10]
    link_bw_filter = [400]
    for experiment_id in all_experiments:
        experiment_dir_path = os.path.join(results_dir, experiment_id)
        # print("Parsing experiment {0}".format(experiment_id))

        # Read setup file to get experiment parameters
        setup_file_path = os.path.join(experiment_dir_path, "setup_details.txt")
        experiment_setup = ExperimentSetup(setup_file_path)
        
        exp_duration = (experiment_setup.job_end_time - experiment_setup.job_start_time)
        print(experiment_setup.experiment_start_time, exp_duration, experiment_setup.experiment_group_desc)


if __name__ == '__main__':
    main()
