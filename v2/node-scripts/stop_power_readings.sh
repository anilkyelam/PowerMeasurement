RESULTS_DIR_FULL_PATH=$1

if [ -f ${RESULTS_DIR_FULL_PATH}/power_readings_process_id ]; 
then     
	PID=$(cat ${RESULTS_DIR_FULL_PATH}/power_readings_process_id)
	kill $PID
	rm ${RESULTS_DIR_FULL_PATH}/power_readings_process_id
fi
