# reset sar
pkill sar

DIR_FULL_PATH=$1

if [ -z "$DIR_FULL_PATH" ]
then
	echo "Please provide directory of source/script files"
	exit -1
fi

# If granularity is not set, default to 1 second.
if [ -z "$2" ]
then
	GRANULARITY=1
else
	GRANULARITY=$2
fi

nohup sar -r ${GRANULARITY}  > ${DIR_FULL_PATH}/memory.sar 2>&1 &
nohup sar -b ${GRANULARITY}  > ${DIR_FULL_PATH}/diskio.sar 2>&1 &
nohup sar -P ALL ${GRANULARITY}  > ${DIR_FULL_PATH}/cpu.sar 2>&1 &
nohup sar -n DEV ${GRANULARITY}  > ${DIR_FULL_PATH}/network.sar 2>&1 &


