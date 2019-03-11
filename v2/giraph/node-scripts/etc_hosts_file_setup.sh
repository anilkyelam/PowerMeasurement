
SCRIPTS_DIR_FULL_PATH=$1
NEW_HOSTS_FILE_NAME=$2
unixstamp=$(date +%s)

# Validate inputs
if [ -z "$SCRIPTS_DIR_FULL_PATH" ]
then
        echo "Please provide directory of source/script files"
        exit -1
fi
if [ -z "$NEW_HOSTS_FILE_NAME" ]
then
        echo "Please provide the name of the new hosts file"
        exit -1
fi

# Copy all new hosts files to /etc/ folder
cp ${SCRIPTS_DIR_FULL_PATH}/hosts_* /etc/

# If checkpoint files does not exist, make one.
if [ ! -f /etc/hosts_chkpt ]; then
	cp /etc/hosts /etc/hosts_chkpt
fi

# Add a snapshot to snapshots folder, just in case.
mkdir -p /etc/hosts_snapshots/
cp /etc/hosts /etc/hosts_snapshots/hosts_chkpt_${unixstamp}

# Replace the hosts file with the new one
cp /etc/${NEW_HOSTS_FILE_NAME} /etc/hosts
