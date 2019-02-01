"""
Utility script to run a custom command on all nodes (like pssh)
"""

import paramiko
from datetime import datetime
import time

# Command to run
command = "tc qdisc show  dev eth0"

old_spark_nodes = ["ccied21", "ccied22", "ccied23", "ccied24", "ccied25", "ccied26", "ccied27", "ccied28", "ccied29"]
new_spark_nodes = ["b09-40", "b09-38", "b09-36", "b09-34", "b09-32", "b09-30", "b09-42", "b09-44"]
spark_nodes_dns_suffx = "sysnet.ucsd.edu"


# Creates SSH client using paramiko lib.
def create_ssh_client(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


# Set rate limit for egress network traffic on each node
def set_network_rate_limit(ssh_client, rate_limit_mbps, password_for_sudo):
    print("Setting rate limit to " + rate_limit_mbps)
    run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    tc_qdisc_show_command = 'tc qdisc show  dev eth0'
    tc_qdisc_set_tbf_rate_limit = 'tc qdisc add dev eth0 root tbf rate {0}mbit burst 1mbit latency 10ms'

    # Delete any non-default traffic control qdisc if it exists
    reset_network_rate_limit(ssh_client, password_for_sudo)

    # Set the rate limit with TBF qdisc
    set_command_with_sudo = run_as_sudo_prefix + tc_qdisc_set_tbf_rate_limit.format(rate_limit_mbps)
    _, _, stderr = ssh_client.exec_command(set_command_with_sudo)
    errors = str(stderr.read())

    # Check if rate limiting is set.
    stdin, stdout, stderr = ssh_client.exec_command(tc_qdisc_show_command)
    output = stdout.read()
    if "rate {0}Mbit".format(rate_limit_mbps) not in str(output):
        raise Exception("Setting link bandwidth failed! " + errors)


# Resets any traffic control qdisc set for a node, which then defaults to pfifo.
def reset_network_rate_limit(ssh_client, password_for_sudo):
    print("Resetting rate limit")
    run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    tc_qdist_reset_command = 'tc qdisc del dev eth0 root'
    cmd_as_sudo = run_as_sudo_prefix + tc_qdist_reset_command
    ssh_client.exec_command(cmd_as_sudo)


# Utility to run a custom script on all the spark nodes
def run_script():
    user_password_info = open("user.pass").readline()   # One line in <user>;<password> format.
    user_name = user_password_info.split(";")[0]
    password = user_password_info.split(";")[1]

    for node_name in new_spark_nodes:
        node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
        ssh_client = create_ssh_client(node_full_name, 22, user_name, password)

        print("=====================================================")
        stdin, stdout, stderr = ssh_client.exec_command("echo $HOSTNAME && ifconfig | grep 'inet 10.'")
        print(stdout.readlines())

        # stdin, stdout, stderr = ssh_client.exec_command("java --version")
        # print(stdout.readlines())

        # set_network_rate_limit(ssh_client, 500, password)
        # reset_network_rate_limit(ssh_client, password)

        # print("=====================================================")
        # stdin, stdout, stderr = ssh_client.exec_command(command)
        # print(stdout.readlines(), stderr.readlines())
        # print("=====================================================")


# Runs Shu-Ting's scripts
def run_sting_script():
    user_password_info = open("user.pass").readline()  # One line in <user>;<password> format.
    user_name = user_password_info.split(";")[0]
    password = user_password_info.split(";")[1]

    for i in [0, 20]:
        for link_rate in [500, 1000]:
            # Set network rate
            if link_rate == 500:
                for node_name in old_spark_nodes:
                    node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
                    ssh_client = create_ssh_client(node_full_name, 22, user_name, password)
                    set_network_rate_limit(ssh_client, 500, password)

            else:
                for node_name in old_spark_nodes:
                    node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
                    ssh_client = create_ssh_client(node_full_name, 22, user_name, password)
                    reset_network_rate_limit(ssh_client, password)

            # Kick off the script
            driver_ssh_client = create_ssh_client("ccied21.sysnet.ucsd.edu", 22, user_name, password)
            print("=====================================================")
            print("Running", i, link_rate, datetime.now)
            stdin, stdout, stderr = driver_ssh_client.exec_command(
                "cd sting_run; sh dev_cluster_submit.sh 4000 128 {0}".format(link_rate))
            print(stdout.readlines(), stderr.readlines())
            print("=====================================================")

            time.sleep(30*60)


if __name__ == '__main__':
    run_script()
    # run_sting_script()

