"""
Runs power measurement experiments on the spark test cluster
"""

import datetime
import paramiko
import os
import time
import json
import traceback
from scp import SCPClient


# Experimental Setup constants
spark_nodes = ["b09-40", "b09-38", "b09-36", "b09-34", "b09-32", "b09-30", "b09-42", "b09-44"]
spark_nodes_dns_suffx = "sysnet.ucsd.edu"
designated_spark_driver_node = "b09-40"
designated_hdfs_master_node = "b09-40"
power_meter_nodes_in_order = []
padding_in_secs = 60


# Since we are dealing with Windows local machine and Linux remote machines
def path_to_linux_style(path):
    return path.replace('\\', '/')
def path_to_windows_style(path):
    return path.replace('/', '\\')


# Remote node constants
remote_home_folder = '/home/ayelam/bf-cluster/sparksort'
remote_scripts_folder = path_to_linux_style(os.path.join(remote_home_folder, "node-scripts"))
remote_results_folder = path_to_linux_style(os.path.join(remote_home_folder, "results"))
etc_folder = "/etc/"
hosts_setup_file = 'etc_hosts_file_setup.sh'
hosts_cleanup_file = 'etc_hosts_file_cleanup.sh'
fat_tree_hosts_file_name = "hosts_fattree"


# Local constants
local_node_scripts_folder = "D:\Git\PowerMeasurement\\v2\\bf_cluster\\node-scripts"
local_results_folder = 'D:\Power Measurements\\v2'
prepare_for_experiment_file = 'prepare_for_experiment.sh'
start_sar_readings_file = 'start_sar_readings.sh'
stop_sar_readings_file = 'stop_sar_readings.sh'
start_power_readings_file = 'start_power_readings.sh'
stop_power_readings_file = 'stop_power_readings.sh'
cleanup_after_experiment_file = 'cleanup_after_experiment.sh'
run_spark_job_file = 'run_spark_job.sh'


# Other constants
run_as_sudo_prefix = 'echo {0} | sudo -S '


# Creates SSH client using paramiko lib.
def create_ssh_client(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


# Create remote folder if it does not exist
def create_folder_if_not_exists(ssh_client, remote_folder_path):
    with ssh_client.open_sftp() as ftp_client:
        try:
            ftp_client.listdir(remote_folder_path)
        except IOError:
            ftp_client.mkdir(remote_folder_path)


# Cutomizes hosts files on all nodes to get all the traffic going through the barefoot switch.
def set_up_hosts_files(root_user_name, password, current_toplogy_hosts_file_name):

    # Setting up network for different topologies to ensure all traffic flows through barefoot switch
    # Copy all hosts files from scripts folder to /etc/ on each node
    # Take a snapshot of /etc/hosts file to revert the changes it after experiments are done
    # Also adding these timed snapshots to hosts_snapshots folder, just in case
    for node_name in spark_nodes:
        node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
        print("Setting up hosts file on " + node_full_name)

        with create_ssh_client(node_full_name, 22, root_user_name, password) as ssh_client:
            sudo_prefix = run_as_sudo_prefix.format(password)
            script_file = path_to_linux_style(os.path.join(remote_scripts_folder, hosts_setup_file))
            _, stdout, stderr = ssh_client.exec_command(sudo_prefix + 'sh {0} {1} {2}'.format(
                script_file, remote_scripts_folder, current_toplogy_hosts_file_name))
            print(stdout.read(), stderr.read())


# Reverts changes to hosts file on all nodes
def clean_up_hosts_files(root_user_name, password):
    for node_name in spark_nodes:
        node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
        print("Cleaning up hosts file on " + node_full_name)

        with create_ssh_client(node_full_name, 22, root_user_name, password) as ssh_client:
            sudo_prefix = run_as_sudo_prefix.format(password)
            script_file = path_to_linux_style(os.path.join(remote_scripts_folder, hosts_cleanup_file))
            _, stdout, stderr = ssh_client.exec_command(sudo_prefix + 'sh {0}'.format(script_file))
            print(stdout.read(), stderr.read())


# Starts HDFS + YARN cluster for spark runs
def start_hdfs_yarn_cluster(hadoop_user_name, hadoop_user_password):
    print("Starting hdfs and yarn cluster")
    master_node_full_name = "{0}.{1}".format(designated_hdfs_master_node, spark_nodes_dns_suffx)
    master_node_ssh_client = create_ssh_client(master_node_full_name, 22, hadoop_user_name, hadoop_user_password)
    _, stdout, stderr = master_node_ssh_client.exec_command("bash hadoop/sbin/start-dfs.sh && bash hadoop/sbin/start-yarn.sh")
    print(stdout.read(), stderr.read())


# Starts HDFS + YARN cluster for spark runs
def stop_hdfs_yarn_cluster(hadoop_user_name, hadoop_user_password):
    print("Stopping hdfs and yarn cluster")
    master_node_full_name = "{0}.{1}".format(designated_hdfs_master_node, spark_nodes_dns_suffx)
    master_node_ssh_client = create_ssh_client(master_node_full_name, 22, hadoop_user_name, hadoop_user_password)
    _, stdout, stderr = master_node_ssh_client.exec_command("bash hadoop/sbin/stop-dfs.sh && bash hadoop/sbin/stop-yarn.sh")
    print(stdout.read(), stderr.read())


# Set up environment for each experiment
def prepare_env_for_experiment(ssh_client, password_for_sudo, input_size_mb, cache_hdfs_file):
    print("Preparing env for experiment")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, prepare_for_experiment_file))
    # run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    _, stdout, stderr = ssh_client.exec_command('bash {0} {1} {2} {3}'.format(script_file, 
                                                                        remote_scripts_folder,
                                                                        input_size_mb,
                                                                        1 if cache_hdfs_file else 0))
    print(stdout.read(), stderr.read())


# Starts SAR readings
def start_sar_readings(ssh_client, node_exp_folder_path, granularity_in_secs=1):
    print("Starting SAR readings")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, start_sar_readings_file))
    _, stdout, stderr = ssh_client.exec_command('bash {0} {1} {2}'.format(script_file, node_exp_folder_path, granularity_in_secs))
    print(stdout.read(), stderr.read())


# Starts spark job with specified algorithm (scala class name) and input size.
def run_spark_job(ssh_client, node_exp_folder_path, input_size_mb, scala_class_name):
    print("Starting spark job")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, run_spark_job_file))
    _, stdout, stderr = ssh_client.exec_command('bash {0} {1} {2} {3} {4}'.format(script_file, remote_scripts_folder, node_exp_folder_path, 
                                                                                input_size_mb, scala_class_name))
    print(stdout.read(), stderr.read())


# Stops SAR readings
def stop_sar_readings(ssh_client):
    print("Stopping SAR readings")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, stop_sar_readings_file))
    _, stdout, stderr = ssh_client.exec_command('bash {0}'.format(script_file))
    print(stdout.read(), stderr.read())


# Cleans up each node after experiment
def cleanup_env_post_experiment(ssh_client):
    print("Cleaning up post environment")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, cleanup_after_experiment_file))
    _, stdout, stderr = ssh_client.exec_command('bash {0}'.format(script_file))
    print(stdout.read(), stderr.read())


# Clears all the data from page cache, dentries and inodes. This is to not let one experiment affect the next one due to caching.
def clear_page_inode_dentries_cache(ssh_client, password_for_sudo):
    print("Clearing all file data caches")
    run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    cache_clear_command = "bash -c 'echo 3 > /proc/sys/vm/drop_caches'"

    cmd_as_sudo = run_as_sudo_prefix + cache_clear_command
    _, stdout, stderr = ssh_client.exec_command(cmd_as_sudo)
    print(stdout.read(), stderr.read())


# Runs a single experiment with specific configurations like input size, network rate, etc.
# Prepares necessary setup to collect readings and runs spark jobs.
def run_experiment(scala_class_name, user_name, user_password, input_size_mb, link_bandwidth_mbps, cache_hdfs_file):
    experiment_start_time = datetime.datetime.now()
    experiment_id = "Exp-" + experiment_start_time.strftime("%Y-%m-%d-%H-%M-%S")

    try:
        experiment_folder_name = experiment_id
        experiment_folder_path = path_to_linux_style(os.path.join(remote_results_folder, experiment_folder_name))

        print("Starting experiment: ", experiment_id)
        driver_node_full_name = "{0}.{1}".format(designated_spark_driver_node, spark_nodes_dns_suffx)
        driver_ssh_client = create_ssh_client(driver_node_full_name, 22, user_name, user_password)

        # Make sure directory structure exists in NFS home folder
        create_folder_if_not_exists(driver_ssh_client, experiment_folder_path)


        # Prepare for experiment. Create input spark files if they do not exist.
        prepare_env_for_experiment(driver_ssh_client, user_password, input_size_mb, cache_hdfs_file)

        # Prepare environment and start collecting readings on each node
        for node_name in spark_nodes:
            node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
            with create_ssh_client(node_full_name, 22, user_name, user_password) as ssh_client:
                node_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, node_name))
                create_folder_if_not_exists(ssh_client, node_exp_folder_path)

                # Clear all kinds of file data from caches
                clear_page_inode_dentries_cache(ssh_client, user_password)

                # Delete any non-default qdisc and set required network rate.
                # reset_network_rate_limit(ssh_client, password)
                # set_network_rate_limit(ssh_client, link_bandwidth_mbps, password)

                start_sar_readings(ssh_client, node_exp_folder_path)

        # Start collecting power readings from the driver node. TODO: No powermeter connected for now.
        # driver_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, designated_driver_node))
        # start_power_readings(driver_ssh_client, driver_exp_folder_path)

        # Wait a bit before the run
        # time.sleep(padding_in_secs)

        # Kick off the run
        spark_job_start_time = datetime.datetime.now()
        driver_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, designated_spark_driver_node))
        run_spark_job(driver_ssh_client, driver_exp_folder_path, input_size_mb, scala_class_name)
        spark_job_end_time = datetime.datetime.now()

        # Wait a bit after the run
        # time.sleep(padding_in_secs)

        # Stop power readings TODO: No powermeter connected for now.
        # stop_power_readings(driver_ssh_client, driver_exp_folder_path)

        # Stop collecting SAR readings on each node
        for node_name in spark_nodes:
            node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
            ssh_client = create_ssh_client(node_full_name, 22, user_name, user_password)
            stop_sar_readings(ssh_client)

        # Copy results to local machine
        with SCPClient(driver_ssh_client.get_transport()) as scp:
            scp.get(experiment_folder_path, local_results_folder, recursive=True)

        # Record experiment setup details for later use
        local_experiment_folder = os.path.join(local_results_folder, experiment_folder_name)
        setup_file = open(os.path.join(local_experiment_folder, "setup_details.txt"), "w")
        json.dump(
            {
                "ExperimentGroup": 1,
                "ExperimentGroupDesc": "First sample runs on bf cluster",
                "ScalaClassName": scala_class_name,
                "InputHdfsCached": cache_hdfs_file,
                
                "ExperimentId": experiment_id,
                "ExperimentStartTime": experiment_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "SparkJobStartTime": spark_job_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "SparkJobEndTime": spark_job_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "AllSparkNodes": spark_nodes,
                "PowerMeterNodesInOrder": power_meter_nodes_in_order,
                "HdfsMasterNode": designated_hdfs_master_node,
                "SparkDriverNode": designated_spark_driver_node,
                "InputSizeGb": input_size_mb/1000.0,
                "LinkBandwidthMbps": link_bandwidth_mbps,
                "PaddingInSecs": padding_in_secs
            }, setup_file, indent=4, sort_keys=True)

        # Cleanup on each node
        cleanup_env_post_experiment(driver_ssh_client)
        for node_name in spark_nodes:
            node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
            # with create_ssh_client(node_full_name, 22, user_name, password) as ssh_client:
            #     reset_network_rate_limit(ssh_client, password)

        driver_ssh_client.close()

        print("Experiment: {0} done!!".format(experiment_id))
        return experiment_id
    except:
        print("Experiment: {0} failed!!".format(experiment_id))
        print(traceback.format_exc())
        return None


# Main
def main():
    scala_class_name = "SortLegacy"
    # scala_class_name = "SortNoDisk"

    input_sizes_mb = [10000, 20000, 30000, 40000, 50000]
    link_bandwidth_mbps = []
    iterations = range(1, 4)

    # Get user creds from temp files
    root_user_password_info = open("root-user.pass").readline()  # One line in <user>;<password> format.
    root_user_name = root_user_password_info.split(";")[0]
    root_password = root_user_password_info.split(";")[1]
    hadoop_user_password_info = open("hadoop-user.pass").readline()  # One line in <user>;<password> format.
    hadoop_user_name = hadoop_user_password_info.split(";")[0]
    hadoop_password = hadoop_user_password_info.split(";")[1]

    master_node_full_name = "{0}.{1}".format(designated_hdfs_master_node, spark_nodes_dns_suffx)
    master_node_ssh_client = create_ssh_client(master_node_full_name, 22, root_user_name, root_password)

    try:
        # Make sure directory structure exists in NFS home folder
        create_folder_if_not_exists(master_node_ssh_client, remote_home_folder)
        create_folder_if_not_exists(master_node_ssh_client, remote_results_folder)

        # Copy source files to NFS
        with SCPClient(master_node_ssh_client.get_transport()) as scp:
            scp.put(local_node_scripts_folder, remote_home_folder, recursive=True)

            # Fix for gensort file losing execute permissions on copying over from windows to linux
            remote_gensort_file_path = path_to_linux_style(os.path.join(remote_scripts_folder, "gensort"))
            master_node_ssh_client.exec_command("chmod +x {0}".format(remote_gensort_file_path))

        # Set up environment for experiments
        set_up_hosts_files(root_user_name, root_password, fat_tree_hosts_file_name)
        start_hdfs_yarn_cluster(hadoop_user_name, hadoop_password)

        # Run all experiments
        for iter_ in iterations:
            for link_bandwidth in link_bandwidth_mbps:
                for input_size_mb in input_sizes_mb:
                    print("Running experiment: {0}, {1}, {2}".format(iter_, input_size_mb, link_bandwidth))
                    run_experiment(scala_class_name, root_user_name, root_password, int(input_size_mb), 
                        link_bandwidth, cache_hdfs_file=False)
                    # time.sleep(1*60)

        print("Environment set up and cluster started!")

    finally:
        # Clean up
        print("Tear down environment? Press [Enter] to continue.")
        input()
        print("Tearing down the environment...")
        
        master_node_ssh_client.close()
        stop_hdfs_yarn_cluster(hadoop_user_name, hadoop_password)
        clean_up_hosts_files(root_user_name, root_password)


if __name__ == '__main__':
    main()
