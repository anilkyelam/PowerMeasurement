SRC_DIR_FULL_PATH=$1
RESULTS_DIR_FULL_PATH=$2


# Start collecting power readings
nohup python3 ${SRC_DIR_FULL_PATH}/powermeter.py ${RESULTS_DIR_FULL_PATH} > ${RESULTS_DIR_FULL_PATH}/power_meter_log 2>&1  &

# Write process id to a temp file for stopping later.
echo $! > ${RESULTS_DIR_FULL_PATH}/power_readings_process_id


