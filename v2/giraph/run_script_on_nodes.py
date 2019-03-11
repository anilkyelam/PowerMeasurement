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
    return output


def create_or_reset_tmpfs_ram_disk(ssh_client, root_password):
    mount_ram_disk_command = "mount -t tmpfs -o size=50G tmpfs /mnt/ramdisk"
    ssh_execute_command(ssh_client, "rm -r -f /mnt/ramdisk", sudo_password=root_password)
    ssh_execute_command(ssh_client, "mkdir /mnt/ramdisk", sudo_password=root_password)
    ssh_execute_command(ssh_client, mount_ram_disk_command, sudo_password=root_password)


# Utility to run a custom script on all the spark nodes
def run_script():
    user_password_info = open("root-user.pass").readline()   # One line in <user>;<password> format.
    user_name = user_password_info.split(";")[0]
    password = user_password_info.split(";")[1]

    for node_name in new_spark_nodes:
        node_full_name = "{0}.{1}".format(node_name, spark_nodes_dns_suffx)
        ssh_client = create_ssh_client(node_full_name, 22, user_name, password)

        ssh_execute_command(ssh_client, "echo $HOSTNAME")
        create_or_reset_tmpfs_ram_disk(ssh_client, password)
        # ssh_execute_command(ssh_client, "shutdown -r", sudo_password=password)

if __name__ == '__main__':
    run_script()

