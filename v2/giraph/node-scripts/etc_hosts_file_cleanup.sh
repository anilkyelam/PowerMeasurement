
# If checkpoint file exists, use it to revert changes to hosts file and remove it, otherwise no action to do.
if [ -f /etc/hosts_chkpt ]; then
	cp /etc/hosts_chkpt /etc/hosts
	rm /etc/hosts_chkpt
fi

