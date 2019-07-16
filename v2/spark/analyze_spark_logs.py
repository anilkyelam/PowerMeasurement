from datetime import datetime
import os
import json
import re
import run_experiments
from plot_one_experiment import ExperimentSetup, setup_details_file_name, results_base_dir
from plot_multiple_experiments import load_all_experiments, power_plots_output_dir
from shutil import copyfile
import matplotlib.pyplot as plt
from itertools import groupby
import numpy as np
from collections import Counter
from sklearn.cluster import KMeans


# Constants
results_base_dir = run_experiments.local_results_folder
spark_log_file = "spark.log"
application_id_regex = '(application_[0-9]+_[0-9]+)'
spark_full_log_file = "spark-detailed.log"


class TaskInfo:
    index=None

    def __init__(self, json_string):
        json_dict = json.loads(json_string)
        self.stage_id = int(json_dict["Stage ID"])
        self.task_id = int(json_dict["Task Info"]["Task ID"])
        self.executor_id = int(json_dict["Task Info"]["Executor ID"])
        self.node = json_dict["Task Info"]["Host"].split('.')[0]
        self.success = not json_dict["Task Info"]["Failed"]
        self.start_time = datetime.fromtimestamp(float(json_dict["Task Info"]["Launch Time"])/1000)
        self.end_time = datetime.fromtimestamp(float(json_dict["Task Info"]["Finish Time"])/1000)
        self.cpu_time_secs = float(json_dict["Task Metrics"]["Executor CPU Time"])/1000000000
        self.gc_time_secs = float(json_dict["Task Metrics"]["JVM GC Time"])/1000
        self.run_time_secs = float(json_dict["Task Metrics"]["Executor Run Time"])/1000
        self.shuffle_time_secs = float(json_dict["Task Metrics"]["Shuffle Read Metrics"].get("Fetch Wait Time", 0))/1000  
        self.shuffle_mbytes = float(json_dict["Task Metrics"]["Shuffle Read Metrics"].get("Remote Bytes Read", 0))*1.0/(1024*1024) 
        self.shuffle_mbytes += float(json_dict["Task Metrics"]["Shuffle Read Metrics"].get("Local Bytes Read", 0))*1.0/(1024*1024)  


    def __str__(self):
        return "{:2d} {:5d} {:1d} {:6s} {:5s} {:s} {:2.2f} {:2.2f} {:2.2f} {:2.2f}".format(
            self.stage_id, self.task_id, self.executor_id, self.node, str(self.success), datetime.strftime(self.start_time, "%Y-%m-%d %H:%M:%S.%f"),  
            self.run_time_secs, self.cpu_time_secs,self.gc_time_secs, self.shuffle_time_secs)


class ExpStats:
    experiment_id = None
    task_counter = None
    run_time_counter = None
    cpu_time_counter = None
    gc_time_counter = None
    gc_task_counter = None
    shuffle_time_counter = None
    shuffle_mbytes_counter = None


def try_get_spark_detailed_log(designated_driver_results_path):
    application_id = None
    spark_simple_log_file_path = os.path.join(designated_driver_results_path, spark_log_file)
    with open(spark_simple_log_file_path, "r") as lines:
        for line in lines:
            matches = re.search(application_id_regex, line)
            if matches:
                application_id = matches.group(1)
                break
    
    if not application_id:
        return False

    spark_log_full_path = os.path.join(results_base_dir, "SparkLogsArchive", application_id)
    if not os.path.exists(spark_log_full_path):
        return False

    copyfile(spark_log_full_path, os.path.join(designated_driver_results_path, spark_full_log_file))
    return True


def parse_spark_detailed_log(results_dir_path, experiment_id, experiment_setup):
    designated_driver_results_path = os.path.join(results_dir_path, experiment_setup.designated_driver_node)
    spark_log_full_path = os.path.join(designated_driver_results_path, spark_full_log_file)

    if not os.path.exists(spark_log_full_path) and not try_get_spark_detailed_log(designated_driver_results_path):
        print("Spark full log file not found for experiment", experiment_id)
        return

    all_tasks = []
    with open(spark_log_full_path, "r") as lines:
        for line in lines:
            # Check for taskend events to get task info
            if 'SparkListenerTaskEnd' in line:
                task = TaskInfo(line)
                # print(task)
                all_tasks.append(task)

    return all_tasks


def plot_spark_task_time(results_dir_path, experiment_id, all_tasks):
    all_stages = set([t.stage_id for t in all_tasks])
    all_nodes = set([t.node for t in all_tasks])
    for stage_id in all_stages:
        sorted_tasks = sorted(filter(lambda t: t.stage_id == stage_id, all_tasks), key=lambda t: t.start_time)
        for idx, val in enumerate(sorted_tasks):    val.index = idx
        base_time = min([t.start_time for t in sorted_tasks])
        # for t in sorted_tasks: print(t)

        fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1)
        fig.set_size_inches(w=15,h=12)
        fig.suptitle("Experiment: {0}, Task running time breakdown for Stage {1}".format(experiment_id, stage_id))

        for node in sorted(all_nodes):
            node_tasks = list(filter(lambda t: t.node == node, sorted_tasks))

            # x = [int((t.start_time - base_time).total_seconds()*1000) for t in node_tasks]
            # x = [t.task_id for t in node_tasks]
            x = [t.index for t in node_tasks]

            y = [t.run_time_secs for t in node_tasks]
            ax1.bar(x, y, label=node)
            ax1.set_ylim(-5, 10)
            ax1.set_ylabel("Run time (secs)")
            
            y = [t.cpu_time_secs for t in node_tasks]
            ax2.bar(x, y, label=node)
            ax2.set_ylim(-5, 10)
            ax2.set_ylabel("CPU time (secs)")

            y = [t.gc_time_secs for t in node_tasks]
            ax3.bar(x, y, label=node)
            ax3.set_ylim(-5, 10)
            ax3.set_ylabel("GC time (secs)")

            y = [t.shuffle_time_secs for t in node_tasks]
            ax4.bar(x, y, label=node)
            ax4.set_ylim(-5, 10)
            ax4.set_ylabel("Shuffle Read time (secs)")
                   
            y = [(t.run_time_secs - t.cpu_time_secs - t.gc_time_secs - t.shuffle_time_secs) for t in node_tasks]
            ax5.bar(x, y, label=node)
            ax5.set_ylim(-5, 10)
            ax5.set_xlabel("Task ID")
            # ax5.set_xlabel("Task Start Time (micro secs)")
            ax5.set_ylabel("Unaccounted (secs)")

        plt.legend()
        output_plot_file_name = "plot_tasks_time_stage_{0}.png".format(stage_id)
        output_full_path = os.path.join(results_dir_path, output_plot_file_name)
        plt.savefig(output_full_path)
        # plt.show()


def print_or_get_exp_task_stats(results_dir_path, experiment_id, all_tasks):
    exp_stats = ExpStats()
    exp_stats.experiment_id = experiment_id

    all_stages = set([t.stage_id for t in all_tasks])
    all_nodes = set([t.node for t in all_tasks])
    for stage_id in all_stages:
        print(experiment_id, ", Stage: ", stage_id)
        sorted_tasks = sorted(filter(lambda t: t.stage_id == stage_id, all_tasks), key=lambda t: t.start_time)
        base_time = min([t.start_time for t in sorted_tasks])
        finish_time = max([t.end_time for t in sorted_tasks])
        
        # Print some aggregate stats
        print("{:20s} {:.2f} secs".format("Total Time", (finish_time - base_time).total_seconds()))
        cpu_times = [t.cpu_time_secs for t in sorted_tasks]
        print("{:20s} {:.2f} secs ({:.2f})".format("CPU Time per Task", np.mean(cpu_times), np.std(cpu_times)))
        gc_times = [t.gc_time_secs for t in sorted_tasks]
        # print(gc_times)
        print("{:20s} {:.2f} secs ({:.2f})".format("GC Time per Task", np.mean(gc_times), np.std(gc_times)))
        shuffle_times = [t.shuffle_time_secs for t in sorted_tasks]
        print("{:20s} {:.2f} secs ({:.2f})".format("Shuffle Wait Time", np.mean(shuffle_times), np.std(shuffle_times)))

        # Print stats per node
        task_counter = Counter()
        tail_task_counter = Counter()
        run_time_counter = Counter()
        cpu_time_counter = Counter()
        gc_time_counter = Counter()
        gc_task_counter = Counter()
        shuffle_time_counter = Counter()
        shuffle_mbytes_counter = Counter()

        for task in sorted_tasks:
            task_counter[task.node] += 1
            tail_task_counter[task.node] += 1 if (finish_time - task.start_time).total_seconds() < 5 else 0
            run_time_counter[task.node] += task.run_time_secs
            cpu_time_counter[task.node] += task.cpu_time_secs
            gc_time_counter[task.node] += task.gc_time_secs
            gc_task_counter[task.node] += 1 if task.gc_time_secs else 0
            shuffle_time_counter[task.node] += task.shuffle_time_secs
            shuffle_mbytes_counter[task.node] += task.shuffle_mbytes

        # Sort by key so that all of them has same x-axis
        task_counter = dict(sorted(task_counter.items()))
        tail_task_counter = dict(sorted(tail_task_counter.items()))
        run_time_counter = dict(sorted(run_time_counter.items()))
        cpu_time_counter = dict(sorted(cpu_time_counter.items()))
        gc_time_counter = dict(sorted(gc_time_counter.items()))
        gc_task_counter = dict(sorted(gc_task_counter.items()))
        shuffle_time_counter = dict(sorted(shuffle_time_counter.items()))
        shuffle_mbytes_counter = dict(sorted(shuffle_mbytes_counter.items()))

        # Get the exact GC runs for each node
        gc_runs = {node: [] for node in gc_task_counter.keys()}
        for node in task_counter.keys():
            gc_impacted_tasks_per_node = [t for t in sorted_tasks if t.node == node and t.gc_time_secs > 0]
            gc_task_times = [(t.start_time - base_time).total_seconds() for t in gc_impacted_tasks_per_node]
            num_runs = int(round(gc_task_counter[node] * 1.0 / 80))
            km = KMeans(n_clusters=num_runs).fit(np.array(gc_task_times).reshape(-1,1))
            task_clusters = {cluster_id:[] for cluster_id in range(num_runs)}
            for idx, task in enumerate(gc_impacted_tasks_per_node):
                task_clusters[km.labels_[idx]].append(task)

            for cluster_id in range(num_runs):
                gc_run_start = int(km.cluster_centers_[cluster_id][0])
                gc_run_duration = np.mean([t.gc_time_secs for t in task_clusters[cluster_id]])
                gc_runs[node].append((gc_run_start, gc_run_duration))


        print("{:10s} {:5s} {:10s} {:10s} {:10s} {:10s} {:10s} {:25s} {:10s} {:15s}".format("Node Name", "Tasks", "Tail Tasks", "Run Time",  "CPU Time", "GC Time", "GC Tasks", "GC Runs (duration)", "Shfl Wait", "Shfl Read (GB)"))
        for key in task_counter.keys():
            print("{:10s} {:5s} {:10s} {:10s} {:10s} {:10s} {:10s} {:25s} {:15s}".format(key, 
                str(task_counter[key]),
                str(tail_task_counter[key]), 
                "{:.2f}".format(run_time_counter[key]),
                "{:.2f}".format(cpu_time_counter[key]/task_counter[key]),
                "{:.2f}".format(gc_time_counter[key]),
                str(gc_task_counter[key]),
                ", ".join(["{:2d}({:.1f})".format(start, dur) for start, dur in sorted(gc_runs[key])]),
                "{:.2f}".format(shuffle_time_counter[key])),
                "{:.2f}".format(shuffle_mbytes_counter[key]/1024))

        # Only collect reduce stage stats for now
        if stage_id == 1:
            exp_stats.task_counter = task_counter
            exp_stats.run_time_counter = run_time_counter
            exp_stats.gc_time_counter = gc_time_counter
            exp_stats.gc_task_counter = gc_task_counter
            exp_stats.shuffle_time_counter = shuffle_time_counter
            exp_stats.cpu_time_counter = cpu_time_counter
            exp_stats.shuffle_mbytes_counter = shuffle_mbytes_counter
        
    return exp_stats


def plot_multiple_exp_stats(results_dir_path, exp_stats_list):

    fig, ax = plt.subplots(1, 1)
    # fig.set_size_inches(w=15,h=12)
    fig.suptitle("CPU time per task across nodes (Reduce Phase)")

    # all_nodes = exp_stats_list[0].task_counter.keys()
    # values = []
    # error = []
    # for node in all_nodes:
    #     # exp_values = [exp.task_counter[node] for exp in exp_stats_list] 
    #     exp_values = [ exp.cpu_time_counter[node]/exp.task_counter[node] for exp in exp_stats_list] 
    #     values.append(np.mean(exp_values))
    #     error.append(np.std(exp_values))

    # ax.errorbar(all_nodes, values, error)

    for exp in exp_stats_list:
        x = exp.task_counter.keys()
        # y = exp.run_time_counter.values()
        y = [val/list(exp.task_counter.values())[idx] for idx, val in enumerate(exp.run_time_counter.values())]
        ax.plot(x, y, label=exp.experiment_id)
        ax.set_ylabel("CPU time per task (secs)")

    plt.legend()
    output_plot_file_name = "plot_cpu_time_per_node_1.png"
    output_full_path = os.path.join(results_dir_path, output_plot_file_name)
    plt.savefig(output_full_path)
    plt.show()


experiments = [
    # "Run-2019-06-30-23-13-44",          # 10 runs to see variation in task scheduling among hdfs nodes
    # "Exp-2019-06-13-18-08-17"
    # "Exp-2019-06-13-18-12-52", "Exp-2019-06-13-18-14-25",     # 300gb sort; 1sec locality wait; 10 vs 40gbps
    # "Exp-2019-06-26-16-58-53"
    # "Exp-2019-05-15-10-06-02" , "Exp-2019-06-26-15-56-41"       # 100gb; 1 numa vs 2 numa sockets per machine
    # "Exp-2019-05-31-18-59-45", "Exp-2019-05-31-19-01-17"      # 200gb sort; 10 vs 40 gbps before perfect hdfs file distribution
    # "Exp-2019-06-13-18-53-16", "Exp-2019-06-13-18-54-36"      # 200gb sort; 10 vs 40gbps after perfect hdfs file distribution
    "Exp-2019-07-10-13-56-23", "Exp-2019-07-10-13-52-47", "Exp-2019-07-10-13-31-29", "Exp-2019-07-10-13-37-28", "Exp-2019-07-10-17-14-23", # 300gb with different numa settings
    # "Exp-2019-07-10-13-52-47"   # Best run as of 7/15
]


def main():
    start_time = datetime.strptime('2019-06-10 00:00:00', "%Y-%m-%d %H:%M:%S")
    end_time = datetime.now()

    exp_stats_list = []
    for experiment in load_all_experiments(start_time, end_time):
        if experiment.experiment_id in experiments or experiment.experiment_group in experiments:
            experiment_id = experiment.experiment_id
            print(experiment_id)
            results_dir_name = experiment_id
            results_dir_path = os.path.join(results_base_dir, results_dir_name)
            setup_file_path = os.path.join(results_dir_path, setup_details_file_name)
            experiment_setup = ExperimentSetup(setup_file_path)
            all_tasks = parse_spark_detailed_log(results_dir_path, experiment_id, experiment_setup)
            # plot_spark_task_time(results_dir_path, experiment_id, all_tasks)
            exp_stats = print_or_get_exp_task_stats(results_dir_path, experiment_id, all_tasks)
            exp_stats_list.append(exp_stats)

    # Plot results collected across multiple experiments
    print("Output plots at path: " + power_plots_output_dir)
    if not os.path.exists(power_plots_output_dir):
        os.mkdir(power_plots_output_dir)
    plot_multiple_exp_stats(power_plots_output_dir, exp_stats_list)


if __name__ == '__main__':
    main()
