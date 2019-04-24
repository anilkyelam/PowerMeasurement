"""
Runs power measurement experiments on hdfs+yarn cluster using giraph jobs
"""

import argparse
import datetime
import paramiko
import os
import time
import json
import sys
import traceback
from scp import SCPClient


# Experimental Setup constants
hdfs_nodes = ["b09-40", "b09-38", "b09-36", "b09-34", "b09-32", "b09-30", "b09-42", "b09-44"]
hdfs_nodes_dns_suffx = "sysnet.ucsd.edu"
designated_giraph_driver_node = "b09-40"
designated_hdfs_master_node = "b09-40"
power_meter_nodes_in_order = []
padding_in_secs = 5
fat_tree_ip_mac_map = {
    "b09-40": ("ec:0d:9a:68:21:c8", "10.0.0.1"),
    "b09-38": ("ec:0d:9a:68:21:c4", "10.0.1.1"),
    "b09-44": ("ec:0d:9a:68:21:a4", "10.1.0.1"),
    "b09-42": ("ec:0d:9a:68:21:ac", "10.1.1.1"),
    "b09-34": ("ec:0d:9a:68:21:a8", "10.2.0.1"),
    "b09-36": ("ec:0d:9a:68:21:a0", "10.2.1.1"),
    "b09-30": ("ec:0d:9a:68:21:b0", "10.3.0.1"),
    "b09-32": ("ec:0d:9a:68:21:84", "10.3.1.1"),
}


# Since we are dealing with Windows local machine and Linux remote machines
def path_to_linux_style(path):
    return path.replace('\\', '/')
def path_to_windows_style(path):
    return path.replace('/', '\\')


# Remote node constants
remote_home_folder = '/home/ayelam/giraph'
remote_scripts_folder = path_to_linux_style(os.path.join(remote_home_folder, "node-scripts"))
remote_results_folder = path_to_linux_style(os.path.join(remote_home_folder, "results"))
etc_folder = "/etc/"
hosts_setup_file = 'etc_hosts_file_setup.sh'
hosts_cleanup_file = 'etc_hosts_file_cleanup.sh'
fat_tree_hosts_file_name = "hosts_fattree"


# Local constants
local_node_scripts_folder = "D:\Git\PowerMeasurement\\v2\\giraph\\node-scripts"
local_results_folder = 'D:\Power Measurements\\v2\\giraph'
prepare_for_experiment_file = 'prepare_for_experiment.sh'
start_sar_readings_file = 'start_sar_readings.sh'
stop_sar_readings_file = 'stop_sar_readings.sh'
start_power_readings_file = 'start_power_readings.sh'
stop_power_readings_file = 'stop_power_readings.sh'
cleanup_after_experiment_file = 'cleanup_after_experiment.sh'
run_giraph_job_file = 'run_giraph_job.sh'


# Creates SSH client using paramiko lib.
def create_ssh_client(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


# Executes command with ssh client and reads from out and err buffers so it does not block.
def ssh_execute_command(ssh_client, command, sudo_password = None): 
    
    if sudo_password:
        run_as_sudo_prefix = 'echo {0} | sudo -S '.format(sudo_password)
        command = run_as_sudo_prefix + command

    _, stdout, stderr = ssh_client.exec_command(command)
    output = str(stdout.read() + stderr.read())
    print(output)
    return output


# Create remote folder if it does not exist
def create_folder_if_not_exists(ssh_client, remote_folder_path):
    with ssh_client.open_sftp() as ftp_client:
        try:
            ftp_client.listdir(remote_folder_path)
        except IOError:
            ftp_client.mkdir(remote_folder_path)


# Sets up each node - adding hosts files, setting IP address, ARP entries, etc.
def set_up_on_each_node(root_user_name, password, current_toplogy_hosts_file_name):

    for node_name in hdfs_nodes:
        node_full_name = "{0}.{1}".format(node_name, hdfs_nodes_dns_suffx)

        with create_ssh_client(node_full_name, 22, root_user_name, password) as ssh_client:    
            print("Setting up hosts file on " + node_full_name)
            script_file = path_to_linux_style(os.path.join(remote_scripts_folder, hosts_setup_file))
            ssh_execute_command(ssh_client, 'sh {0} {1} {2}'.format(script_file, remote_scripts_folder, current_toplogy_hosts_file_name),
                                    sudo_password=password)

            print("Setting IP address and adding ARP entries on " + node_name)
            set_ip_address = ["ifconfig enp101s0 '{0}' netmask 255.0.0.0".format(fat_tree_ip_mac_map[node_name][1])]
            set_arp_table_entries = ["arp -s {0} {1}".format(fat_tree_ip_mac_map[node][1], fat_tree_ip_mac_map[node][0]) 
                                            for node in fat_tree_ip_mac_map.keys() if node != node_name]
            for cmd in (set_ip_address + set_arp_table_entries):
                ssh_execute_command(ssh_client, cmd, sudo_password=password)


# Reverts changes on each node - cleanup to hosts file on all nodes, resets TC, refreshes link interface, etc.
def clean_up_on_each_node(root_user_name, password):
    for node_name in hdfs_nodes:
        node_full_name = "{0}.{1}".format(node_name, hdfs_nodes_dns_suffx)
        with create_ssh_client(node_full_name, 22, root_user_name, password) as ssh_client:
            
            print("Cleaning up hosts file on " + node_full_name)
            script_file = path_to_linux_style(os.path.join(remote_scripts_folder, hosts_cleanup_file))
            ssh_execute_command(ssh_client, 'sh {0}'.format(script_file), sudo_password=password)
             
            print("Resetting TC and network interface on " + node_full_name)
            reset_network_rate_limit(ssh_client, password)
            refresh_link_interface(ssh_client, password)


# Starts HDFS + YARN cluster for spark runs
def start_hdfs_yarn_cluster(hadoop_user_name, hadoop_user_password):
    print("Starting hdfs and yarn cluster")
    master_node_full_name = "{0}.{1}".format(designated_hdfs_master_node, hdfs_nodes_dns_suffx)
    master_node_ssh_client = create_ssh_client(master_node_full_name, 22, hadoop_user_name, hadoop_user_password)
    ssh_execute_command(master_node_ssh_client, "bash hadoop/sbin/start-dfs.sh && bash hadoop/sbin/start-yarn.sh")

    # Check if the cluster is up and running properly i.e., all the data nodes are up
    output = ssh_execute_command(master_node_ssh_client, "hadoop/bin/hdfs dfsadmin -report")
    errors = ["Node {0} not found in the cluster report\n".format(node_name) for node_name in hdfs_nodes 
        if node_name != designated_hdfs_master_node and node_name not in output] 
    if errors.__len__() > 0:
        print(errors)
        raise Exception("Cluster is not configured properly!")
    print ("Cluster is up and running!")


# Starts HDFS + YARN cluster for spark runs
def stop_hdfs_yarn_cluster(hadoop_user_name, hadoop_user_password):
    # Remove cache directives so that when HDFS starts up again, your prepare_env_script doesn't think the file is already in cache
    print("Removing hdfs cache directives of input files")
    master_node_full_name = "{0}.{1}".format(designated_hdfs_master_node, hdfs_nodes_dns_suffx)
    master_node_ssh_client = create_ssh_client(master_node_full_name, 22, hadoop_user_name, hadoop_user_password)
    ssh_execute_command(master_node_ssh_client, "hadoop/bin/hdfs cacheadmin -removeDirectives -path '/user/ayelam'")

    print("Stopping hdfs and yarn cluster")
    ssh_execute_command(master_node_ssh_client, "bash hadoop/sbin/stop-yarn.sh && bash hadoop/sbin/stop-dfs.sh")


# Set up environment for each experiment
def prepare_env_for_experiment(ssh_client, password_for_sudo):
    print("Preparing env for experiment")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, prepare_for_experiment_file))
    ssh_execute_command(ssh_client, 'bash {0}'.format(script_file))


# Starts SAR readings
def start_sar_readings(ssh_client, node_exp_folder_path, granularity_in_secs=1):
    print("Starting SAR readings")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, start_sar_readings_file))
    ssh_execute_command(ssh_client, 'bash {0} {1} {2}'.format(script_file, node_exp_folder_path, granularity_in_secs))


# Starts giraph job with specified algorithm (giraph class name) and input graph.
def run_giraph_job(ssh_client, node_exp_folder_path, giraph_class_name, input_graph_name):
    print("Starting giraph job")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, run_giraph_job_file))
    ssh_execute_command(ssh_client, 'bash {0} {1} {2} {3} {4}'.format(script_file, remote_scripts_folder, node_exp_folder_path, 
        giraph_class_name, input_graph_name))


# Stops SAR readings
def stop_sar_readings(ssh_client):
    print("Stopping SAR readings")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, stop_sar_readings_file))
    ssh_execute_command(ssh_client, 'bash {0}'.format(script_file))


# Cleans up each node after experiment
def cleanup_env_post_experiment(ssh_client):
    print("Cleaning up post environment")
    script_file = path_to_linux_style(os.path.join(remote_scripts_folder, cleanup_after_experiment_file))
    ssh_execute_command(ssh_client, 'bash {0}'.format(script_file))


# Clears all the data from page cache, dentries and inodes. This is to not let one experiment affect the next one due to caching.
def clear_page_inode_dentries_cache(ssh_client, password_for_sudo):
    print("Clearing all file data caches")
    cache_clear_command = "bash -c 'echo 3 > /proc/sys/vm/drop_caches'"
    ssh_execute_command(ssh_client, cache_clear_command, sudo_password=password_for_sudo)


# Set rate limit for egress network traffic on each node
def set_network_rate_limit(ssh_client, rate_limit_mbps, password_for_sudo):

    if rate_limit_mbps == 0:
        print("Not setting any network rate limit")
        return

    # Set the rate limit with TBF qdisc
    print("Setting network rate limit to {0} mbps".format(rate_limit_mbps))
    tc_qdisc_set_tbf_rate_limit = 'tc qdisc add dev enp101s0 root tbf rate {0}mbit burst 1mbit latency 10ms'
    ssh_execute_command(ssh_client, tc_qdisc_set_tbf_rate_limit.format(rate_limit_mbps), sudo_password=password_for_sudo)

    # Check if rate limiting is properly set.
    tc_qdisc_show_command = 'tc qdisc show  dev enp101s0'
    output = ssh_execute_command(ssh_client, tc_qdisc_show_command)
    token_rate_text = "rate {0}Mbit".format(rate_limit_mbps) if rate_limit_mbps % 1000 != 0 \
        else "rate {0}Gbit".format(int(rate_limit_mbps/1000))
    if "tbf" not in output or token_rate_text not in output:
        raise Exception("Setting link bandwidth failed!")


# Resets any traffic control qdisc set for a node, which then defaults to pfifo.
def reset_network_rate_limit(ssh_client, password_for_sudo):
    print("Resetting network rate limit, deleting any custom qdisc")
    tc_qdist_reset_command = 'tc qdisc del dev enp101s0 root'
    ssh_execute_command(ssh_client, tc_qdist_reset_command, sudo_password=password_for_sudo)


# Refreshes interface (Reloads the NIC driver?), clears up any mess that TC makes  
def refresh_link_interface(ssh_client, password_for_sudo):
    print("Refreshing the link interface")
    set_if_down_command = 'ip link set enp101s0 down'
    set_if_up_command = 'ip link set enp101s0 up'

    ssh_execute_command(ssh_client, set_if_down_command, sudo_password=password_for_sudo)
    ssh_execute_command(ssh_client, set_if_up_command, sudo_password=password_for_sudo)


# Runs a single experiment with specific configurations like input graph and network rate
def run_experiment(exp_run_id, exp_run_desc, giraph_class_name, root_user_name, root_password, 
                        hadoop_user_name, hadoop_password, input_graph_name, link_bandwidth_mbps, cache_hdfs_file):
    experiment_start_time = datetime.datetime.now()
    experiment_id = "Exp-" + experiment_start_time.strftime("%Y-%m-%d-%H-%M-%S")

    try:
        experiment_folder_name = experiment_id
        experiment_folder_path = path_to_linux_style(os.path.join(remote_results_folder, experiment_folder_name))

        print("Starting experiment: ", experiment_id)
        driver_node_full_name = "{0}.{1}".format(designated_giraph_driver_node, hdfs_nodes_dns_suffx)
        driver_ssh_client = create_ssh_client(driver_node_full_name, 22, root_user_name, root_password)

        # Make sure directory structure exists in NFS home folder
        create_folder_if_not_exists(driver_ssh_client, experiment_folder_path)

        # Prepare for experiment. Create input spark files if they do not exist.
        prepare_env_for_experiment(driver_ssh_client, root_password)

        # Prepare environment and start collecting readings on each node
        for node_name in hdfs_nodes:
            node_full_name = "{0}.{1}".format(node_name, hdfs_nodes_dns_suffx)
            with create_ssh_client(node_full_name, 22, root_user_name, root_password) as ssh_client:
                print("Setting up for giraph job on node " + node_name)
                node_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, node_name))
                create_folder_if_not_exists(ssh_client, node_exp_folder_path)

                # Clear all kinds of file data from caches
                clear_page_inode_dentries_cache(ssh_client, root_password)

                # Delete any non-default qdisc and set required network rate.
                reset_network_rate_limit(ssh_client, root_password)
                set_network_rate_limit(ssh_client, link_bandwidth_mbps, root_password)

                start_sar_readings(ssh_client, node_exp_folder_path)

        # Start collecting power readings from the driver node. TODO: No powermeter connected for now.
        # driver_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, designated_driver_node))
        # start_power_readings(driver_ssh_client, driver_exp_folder_path)

        # Wait a bit before the run
        time.sleep(padding_in_secs)

        # Kick off the run
        giraph_job_start_time = datetime.datetime.now()
        driver_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, designated_giraph_driver_node))
        
        with create_ssh_client(driver_node_full_name, 22, hadoop_user_name, hadoop_password) as ssh_client:
            run_giraph_job(ssh_client, driver_exp_folder_path, giraph_class_name, input_graph_name)
        # input("Press [Enter] to continue.")

        giraph_job_end_time = datetime.datetime.now()

        # Wait a bit after the run
        time.sleep(padding_in_secs)

        # Stop power readings TODO: No powermeter connected for now.
        # stop_power_readings(driver_ssh_client, driver_exp_folder_path)

        # Stop collecting SAR readings on each node
        for node_name in hdfs_nodes:
            node_full_name = "{0}.{1}".format(node_name, hdfs_nodes_dns_suffx)
            with create_ssh_client(node_full_name, 22, root_user_name, root_password) as ssh_client:
                stop_sar_readings(ssh_client)

        # Copy results to local machine
        with SCPClient(driver_ssh_client.get_transport()) as scp:
            scp.get(experiment_folder_path, local_results_folder, recursive=True)

        # Record experiment setup details for later use
        local_experiment_folder = os.path.join(local_results_folder, experiment_folder_name)
        setup_file = open(os.path.join(local_experiment_folder, "setup_details.txt"), "w")
        json.dump(
            {
                "ExperimentGroup": exp_run_id,
                "ExperimentGroupDesc": exp_run_desc,
                "GiraphClassName": giraph_class_name,
                "InputHdfsCached": cache_hdfs_file,
                
                "ExperimentId": experiment_id,
                "ExperimentStartTime": experiment_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "GiraphJobStartTime": giraph_job_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "GiraphJobEndTime": giraph_job_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "HdfsNodes": hdfs_nodes,
                "PowerMeterNodesInOrder": power_meter_nodes_in_order,
                "HdfsMasterNode": designated_hdfs_master_node,
                "GiraphDriverNode": designated_giraph_driver_node,
                "InputGraphFile": input_graph_name,
                "LinkBandwidthMbps": link_bandwidth_mbps,
                "PaddingInSecs": padding_in_secs,
                "Comments": ""
            }, setup_file, indent=4, sort_keys=True)

        # Cleanup on each node
        cleanup_env_post_experiment(driver_ssh_client)
        driver_ssh_client.close()

        print("Experiment: {0} done!!".format(experiment_id))
        return experiment_id
    except:
        print("Experiment: {0} failed!!".format(experiment_id))
        print(traceback.format_exc())
        return None


def copy_src_files(root_user_name, root_password):
    print("Copying source files to NFS")
    master_node_full_name = "{0}.{1}".format(designated_hdfs_master_node, hdfs_nodes_dns_suffx)
    with create_ssh_client(master_node_full_name, 22, root_user_name, root_password) as master_node_ssh_client:
        # Make sure the directory structure exists in NFS home folder
        create_folder_if_not_exists(master_node_ssh_client, remote_home_folder)
        create_folder_if_not_exists(master_node_ssh_client, remote_results_folder)

        # Copy source files to NFS
        with SCPClient(master_node_ssh_client.get_transport()) as scp:
            scp.put(local_node_scripts_folder, remote_home_folder, recursive=True)


# Set up environment for experiments
def setup_env(root_user_name, root_password, hadoop_user_name, hadoop_password):
    set_up_on_each_node(root_user_name, root_password, fat_tree_hosts_file_name)
    start_hdfs_yarn_cluster(hadoop_user_name, hadoop_password)


# Run experiments
def run(root_user_name, root_password, hadoop_user_name, hadoop_password, exp_run_desc):
    giraph_class_name = "SimplePageRankComputation"

    input_graph_files = [ "darwini-10b-edges" ] # "uk-2007-05.graph-txt", "twitter.graph-txt", "darwini-2b-edges", "darwini-5b-edges" ]
    link_bandwidth_mbps = [10000]   # [200, 500, 1000, 2000, 3000, 5000, 8000, 10000]
    iterations = range(1, 2)
    cache_hdfs_input = False

    # Command line arguments
    exp_run_id = "Run-" + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # Run all experiments
    for iter_ in iterations:
        for link_bandwidth in link_bandwidth_mbps:
            for input_graph_name in input_graph_files:
                print("Running experiment: {0}, {1}, {2}".format(iter_, input_graph_name, link_bandwidth))
                run_experiment(exp_run_id, exp_run_desc, giraph_class_name, root_user_name, root_password, hadoop_user_name,
                    hadoop_password, input_graph_name, link_bandwidth, cache_hdfs_file=cache_hdfs_input)
                # time.sleep(1*60)


def teardown_env(root_user_name, root_password, hadoop_user_name, hadoop_password):  
    print("Tearing down the environment...")      
    stop_hdfs_yarn_cluster(hadoop_user_name, hadoop_password)
    clean_up_on_each_node(root_user_name, root_password)


def main():
    # Get user creds from temp files
    root_user_password_info = open("root-user.pass").readline()  # One line in <user>;<password> format.
    root_user_name = root_user_password_info.split(";")[0]
    root_password = root_user_password_info.split(";")[1]
    hadoop_user_password_info = open("hadoop-user.pass").readline()  # One line in <user>;<password> format.
    hadoop_user_name = hadoop_user_password_info.split(";")[0]
    hadoop_password = hadoop_user_password_info.split(";")[1]

    # Parse args and call relevant action
    parser = argparse.ArgumentParser("Runs power measurement experiments on hdfs+yarn cluster using giraph jobs")
    parser.add_argument('--setup', action='store_true', help='sets up environment, including bringing up hadoop cluster')
    parser.add_argument('--teardown', action='store_true', help='clean up environment, including removing hadoop cluster')
    parser.add_argument('--refresh', action='store_true', help='copy updated source scripts to NFS')
    parser.add_argument('--run', action='store_true', help='runs experiments')
    parser.add_argument('--desc', action='store', help='description for the current runs')
    args = parser.parse_args()

    if args.setup:
        setup_env(root_user_name, root_password, hadoop_user_name, hadoop_password)
        copy_src_files(root_user_name, root_password)

    if args.refresh:
        copy_src_files(root_user_name, root_password)

    if args.run:
        assert args.desc is not None, 'Provide description with --desc parameter for this run!'
        run(root_user_name, root_password, hadoop_user_name, hadoop_password, args.desc)

    if args.teardown:
        teardown_env(root_user_name, root_password, hadoop_user_name, hadoop_password)


if __name__ == '__main__':
    main()
