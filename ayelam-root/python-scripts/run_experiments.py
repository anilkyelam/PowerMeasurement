"""
Runs power measurement experiments on a spark cluster
"""

import datetime
import paramiko
import os
import time
import json
import traceback
from scp import SCPClient
import plot_one_experiment


# Experimental Setup constants - rarely vary across experiments
spark_nodes = ["ccied21", "ccied22", "ccied23", "ccied24", "ccied25", "ccied26", "ccied27", "ccied28", "ccied29"]
spark_nodes_dns_suffx = "sysnet.ucsd.edu"
designated_driver_node = "ccied21"
power_meter_nodes_in_order = ["ccied21", "ccied22", "ccied23", "ccied24"]
padding_in_secs = 0


# Since we are dealing with Windows local machine and Linux remote machines
def path_to_linux_style(path):
    return path.replace('\\', '/')
def path_to_windows_style(path):
    return path.replace('/', '\\')


# Remote node constants
remote_home_folder = '/home/ayelam/sparksort/'
remote_results_folder = '/home/ayelam/sparksort/results'
source_code_folder_name = 'src'
source_code_folder_path = path_to_linux_style(os.path.join(remote_home_folder, source_code_folder_name))


# Local node constants
local_source_folder = "D:\src"
local_results_folder = "D:\Power Measurements"
prepare_for_experiment_file = 'prepare_for_experiment.sh'
start_sar_readings_file = 'start_sar_readings.sh'
stop_sar_readings_file = 'stop_sar_readings.sh'
start_power_readings_file = 'start_power_readings.sh'
stop_power_readings_file = 'stop_power_readings.sh'
cleanup_after_experiment_file = 'cleanup_after_experiment.sh'
run_spark_job_file = 'run_spark_job.sh'


# Other constants


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


# Cleanup to perform before experiment
def prepare_env_for_experiment(ssh_client, input_size_mb):
    print("Preparing env for experiment")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, prepare_for_experiment_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0} {1} {2}'.format(script_file, source_code_folder_path, input_size_mb))
    print(stdout.read(), stderr.read())


# Starts SAR readings
def start_sar_readings(ssh_client, node_exp_folder_path, granularity_in_secs=1):
    print("Starting SAR readings")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, start_sar_readings_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0} {1} {2}'.format(script_file, node_exp_folder_path, granularity_in_secs))
    print(stdout.read(), stderr.read())


# Starts Power readings
def start_power_readings(ssh_client, node_exp_folder_path):
    print("Starting power readings")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, start_power_readings_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0} {1} {2}'.format(script_file, source_code_folder_path, node_exp_folder_path))
    print(stdout.read(), stderr.read())


# Starts spark job with specified algorithm (scala class name) and input size.
def run_spark_job(ssh_client, node_exp_folder_path, input_size_mb, scala_class_name):
    print("Starting spark job")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, run_spark_job_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0} {1} {2} {3} {4}'.format(script_file, source_code_folder_path,
                                                        node_exp_folder_path, input_size_mb, scala_class_name))
    print(stdout.read(), stderr.read())


# Stops SAR readings
def stop_sar_readings(ssh_client):
    print("Stopping SAR readings")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, stop_sar_readings_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0}'.format(script_file))
    print(stdout.read(), stderr.read())


# Stops Power readings
def stop_power_readings(ssh_client, node_exp_folder_path):
    print("Stopping power readings")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, stop_power_readings_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0} {1}'.format(script_file, node_exp_folder_path))
    print(stdout.read(), stderr.read())


# Cleans up each node after experiment
def cleanup_env_post_experiment(ssh_client):
    print("Cleaning up post environment")
    script_file = path_to_linux_style(os.path.join(source_code_folder_path, cleanup_after_experiment_file))
    _, stdout, stderr = ssh_client.exec_command('sh {0}'.format(script_file))
    print(stdout.read(), stderr.read())


# Set rate limit for egress network traffic on each node
def set_network_rate_limit(ssh_client, rate_limit_mbps, password_for_sudo):
    print("Setting network rate limit to {0} mbps".format(rate_limit_mbps))
    run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    tc_qdisc_show_command = 'tc qdisc show  dev eth0'
    tc_qdisc_set_tbf_rate_limit = 'tc qdisc add dev eth0 root tbf rate {0}mbit burst 1mbit latency 10ms'

    # Set the rate limit with TBF qdisc
    set_command_with_sudo = run_as_sudo_prefix + tc_qdisc_set_tbf_rate_limit.format(rate_limit_mbps)
    _, stdout, stderr = ssh_client.exec_command(set_command_with_sudo)
    print(stdout.read(), stderr.read())

    # Check if rate limiting is set.
    stdin, stdout, stderr = ssh_client.exec_command(tc_qdisc_show_command)
    output = str(stdout.read())
    token_rate_text = "rate {0}Mbit".format(rate_limit_mbps) if rate_limit_mbps % 1000 != 0 \
        else "rate {0}Gbit".format(int(rate_limit_mbps/1000))
    if "tbf" not in output or token_rate_text not in output:
        raise Exception("Setting link bandwidth failed!")


# Resets any traffic control qdisc set for a node, which then defaults to pfifo.
def reset_network_rate_limit(ssh_client, password_for_sudo):
    print("Resetting network rate limit, deleting any custom qdisc")
    run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    tc_qdist_reset_command = 'tc qdisc del dev eth0 root'

    cmd_as_sudo = run_as_sudo_prefix + tc_qdist_reset_command
    _, stdout, stderr = ssh_client.exec_command(cmd_as_sudo)
    print(stdout.read(), stderr.read())


# Prepares necessary setup to collect readings and runs spark jobs.
def run_experiment(scala_class_name, input_size_mb, link_bandwidth_mbps):
    experiment_start_time = datetime.datetime.now()
    experiment_id = "Exp-" + experiment_start_time.strftime("%Y-%m-%d-%H-%M-%S")

    try:
        experiment_folder_name = experiment_id
        experiment_folder_path = path_to_linux_style(os.path.join(remote_results_folder, experiment_folder_name))

        print("Starting experiment: ", experiment_id)
        user_password_info = open("user_pass.txt").readline()   # One line in <user>;<password> format.
        user_name = user_password_info.split(";")[0]
        password = user_password_info.split(";")[1]
        driver_node_full_name = "{0}.{1}".format(designated_driver_node, spark_nodes_dns_suffx)
        driver_ssh_client = create_ssh_client(driver_node_full_name, 22, user_name, password)

        # Make sure directory structure exists in NFS home folder
        create_folder_if_not_exists(driver_ssh_client, remote_home_folder)
        create_folder_if_not_exists(driver_ssh_client, remote_results_folder)
        create_folder_if_not_exists(driver_ssh_client, experiment_folder_path)

        # Copy source files
        with SCPClient(driver_ssh_client.get_transport()) as scp:
            scp.put(local_source_folder, remote_home_folder, recursive=True)

            # Fix for gensort losing execute tags on copying over from windows to linux
            remote_gensort_file_path = path_to_linux_style(os.path.join(source_code_folder_path, "gensort"))
            driver_ssh_client.exec_command("chmod +x {0}".format(remote_gensort_file_path))

        # Prepare for experiment. Create input spark files if they do not exist.
        prepare_env_for_experiment(driver_ssh_client, input_size_mb)

        # Prepare environment and start collecting readings on each node
        for node_name in spark_nodes:
            node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
            with create_ssh_client(node_full_name, 22, user_name, password) as ssh_client:
                node_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, node_name))
                create_folder_if_not_exists(ssh_client, node_exp_folder_path)

                # Delete any non-default qdisc and set required network rate.
                reset_network_rate_limit(ssh_client, password)
                set_network_rate_limit(ssh_client, link_bandwidth_mbps, password)

                start_sar_readings(ssh_client, node_exp_folder_path)

        # Start collecting power readings from the driver node
        driver_exp_folder_path = path_to_linux_style(os.path.join(experiment_folder_path, designated_driver_node))
        start_power_readings(driver_ssh_client, driver_exp_folder_path)

        # Wait a bit before the run
        time.sleep(padding_in_secs)

        # Kick off the run
        spark_job_start_time = datetime.datetime.now()
        run_spark_job(driver_ssh_client, driver_exp_folder_path, input_size_mb, scala_class_name)
        spark_job_end_time = datetime.datetime.now()

        # Wait a bit after the run
        time.sleep(padding_in_secs)

        # Stop power readings
        stop_power_readings(driver_ssh_client, driver_exp_folder_path)

        # Stop collecting SAR readings on each node
        for node_name in spark_nodes:
            node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
            ssh_client = create_ssh_client(node_full_name, 22, user_name, password)
            stop_sar_readings(ssh_client)

        # Copy results to local machine
        with SCPClient(driver_ssh_client.get_transport()) as scp:
            scp.get(experiment_folder_path, local_results_folder, recursive=True)

        # Record experiment setup details for later use
        local_experiment_folder = os.path.join(local_results_folder, experiment_folder_name)
        setup_file = open(os.path.join(local_experiment_folder, "setup_details.txt"), "w")
        json.dump(
            {
                "ExperimentId": experiment_id,
                "ExperimentStartTime": experiment_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "SparkJobStartTime": spark_job_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "SparkJobEndTime": spark_job_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "AllSparkNodes": spark_nodes,
                "PowerMeterNodesInOrder": power_meter_nodes_in_order,
                "DriverNode": designated_driver_node,
                "InputSizeGb": input_size_mb/1000.0,
                "LinkBandwidthMbps": link_bandwidth_mbps,
                "PaddingInSecs": padding_in_secs,
                "ScalaClassName": scala_class_name,
                "InputHdfsCached": True
            }, setup_file, indent=4, sort_keys=True)

        # Cleanup on each node
        cleanup_env_post_experiment(driver_ssh_client)
        for node_name in spark_nodes:
            node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
            with create_ssh_client(node_full_name, 22, user_name, password) as ssh_client:
                reset_network_rate_limit(ssh_client, password)

        driver_ssh_client.close()

        print("Experiment: {0} done!!",format(experiment_id))
        return experiment_id
    except:
        print("Experiment: {0} failed!!",format(experiment_id))
        print(traceback.format_exc())
        return None


def main():
    scala_class_name = "SortNoDisk"
    input_sizes_mb = [20000]
    link_bandwidth_mbps = [400]
    iterations = range(1, 4)

    # input_sizes_mb = [1000]
    # link_bandwidth_mbps = [200]
    # iterations = [1]

    # run_experiment(input_size_gb=10, link_bandwidth_mbps=500)
    for input_size_mb in input_sizes_mb:
        for link_bandwidth in link_bandwidth_mbps:
            for _ in iterations:
                print("Running experiment: {0}, {1}".format(input_size_mb, link_bandwidth))
                experiment_id = run_experiment(scala_class_name, int(input_size_mb), link_bandwidth)
                # if experiment_id is not None:
                    # parse_and_plot_results.parse_and_plot_results(experiment_id)


if __name__ == '__main__':
    main()
