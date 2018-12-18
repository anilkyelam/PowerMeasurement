"""
Utility script to run a custom command on all nodes
"""

import paramiko

# Command to run
command = "tc qdisc show  dev eth0"

spark_nodes = ["ccied21", "ccied22", "ccied23", "ccied24", "ccied25", "ccied26", "ccied27", "ccied28", "ccied29"]
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
    run_as_sudo_prefix = 'echo {0} | sudo -S '.format(password_for_sudo)
    tc_qdist_reset_command = 'tc qdisc del dev eth0 root'
    cmd_as_sudo = run_as_sudo_prefix + tc_qdist_reset_command
    ssh_client.exec_command(cmd_as_sudo)


# Utility to run a custom script on all the spark nodes
def run_script():
    user_password_info = open("user_pass.txt").readline()   # One line in <user>;<password> format.
    user_name = user_password_info.split(";")[0]
    password = user_password_info.split(";")[1]

    for node_name in spark_nodes:
        node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
        ssh_client = create_ssh_client(node_full_name, 22, user_name, password)

        print("=====================================================")
        stdin, stdout, stderr = ssh_client.exec_command("echo $HOSTNAME")
        print(stdout.readlines())

        # set_network_rate_limit(ssh_client, 300, password)
        reset_network_rate_limit(ssh_client, password)

        # print("=====================================================")
        # stdin, stdout, stderr = ssh_client.exec_command(command)
        # print(stdout.readlines(), stderr.readlines())
        # print("=====================================================")


if __name__ == '__main__':
    run_script()

