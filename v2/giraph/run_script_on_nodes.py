"""
Utility script to run a custom command on all nodes (like pssh)
"""

import paramiko
from datetime import datetime
import time

new_spark_nodes = ["b09-40", "b09-38", "b09-36", "b09-34", "b09-32", "b09-30", "b09-42", "b09-44"]
spark_nodes_dns_suffx = "sysnet.ucsd.edu"


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
    return output`


def create_or_reset_tmpfs_ram_disk(ssh_client, root_password):
    mount_ram_disk_command = "mount -t tmpfs -o size=50G tmpfs /mnt/ramdisk"
    ssh_execute_command(ssh_client, "rm -r -f /mnt/ramdisk", sudo_password=root_password)
    ssh_execute_command(ssh_client, "mkdir /mnt/ramdisk", sudo_password=root_password)
    ssh_execute_command(ssh_client, mount_ram_disk_command, sudo_password=root_password)


# Utility to run a custom script on all the spark nodes
def run_script():
    # Get user creds from temp files
    root_user_password_info = open("root-user.pass").readline()  # One line in <user>;<password> format.
    root_user_name = root_user_password_info.split(";")[0]
    root_password = root_user_password_info.split(";")[1]
    hadoop_user_password_info = open("hadoop-user.pass").readline()  # One line in <user>;<password> format.
    hadoop_user_name = hadoop_user_password_info.split(";")[0]
    hadoop_password = hadoop_user_password_info.split(";")[1]

    for node_name in new_spark_nodes:
        node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
        ssh_client = create_ssh_client(node_full_name, 22, root_user_name, root_password)

        ssh_execute_command(ssh_client, "echo $HOSTNAME")
        create_or_reset_tmpfs_ram_disk(ssh_client, root_password)
        ssh_execute_command(ssh_client, "ps -u hadoop", sudo_password=root_password)
        # ssh_execute_command(ssh_client, "shutdown -r", sudo_password=root_password)
 
        hdp_user_ssh_client = create_ssh_client(node_full_name, 22, hadoop_user_name, hadoop_password)
        ssh_execute_command(hdp_user_ssh_client, "pkill java")

if __name__ == '__main__':
    run_script()

