SRC_DIR_FULL_PATH=$1
RESULTS_DIR_FULL_PATH=$2
SIZE_IN_MB=$3
SCALA_CLASS_NAME=$4
RECORD_SIZE_BYTES=$5
FINAL_PARTITION_COUNT=$6

# Needed when running from sshclient in Python
# export JAVA_HOME=/usr/lib/jvm/java-8-oracle; 

# Run spark job
spark-submit --class PowerMeasurements.${SCALA_CLASS_NAME} \
        --num-executors 16 --executor-cores 40 --executor-memory 75g --driver-cores 5 --driver-memory 5g  \
        "${SRC_DIR_FULL_PATH}/target/scala-2.11/sparksort_2.11-0.1.jar" yarn ${SIZE_IN_MB} ${RECORD_SIZE_BYTES} ${FINAL_PARTITION_COUNT}  \
        > /mnt/ramdisk/spark.log 2>&1

# Get application id from spark stdout log and get the detailed log file from spark logs in hdfs
application_id=$(grep -E -oh "(application_[0-9]+_[0-9]+)" /mnt/ramdisk/spark.log | head -1)
if [[ ! -z ${application_id} ]]; then hdfs dfs -get /spark/spark-logs/${application_id} ${RESULTS_DIR_FULL_PATH}/spark-detailed.log; fi

# Move log file to results folder
mv /mnt/ramdisk/spark.log ${RESULTS_DIR_FULL_PATH}


# Command for SortLegacy.
# spark-submit --class PowerMeasurements.${SCALA_CLASS_NAME} \
#         --num-executors 40 --executor-cores 5 --executor-memory 10g --driver-cores 5 --driver-memory 5g \
#         "${SRC_DIR_FULL_PATH}/target/scala-2.11/sparksort_2.11-0.1.jar" \
#         yarn "/user/ayelam/sort_inputs/${SIZE_IN_MB}mb.input" "/user/ayelam/sort_outputs/${SIZE_IN_MB}mb.output" \
#         "/user/ayelam/sort_stats/${SIZE_IN_MB}mb.stats" > ${RESULTS_DIR_FULL_PATH}/spark.log 2>&1

# spark-submit --class PowerMeasurements.TeraSort \
#         --num-executors 8 --executor-cores 80 --executor-memory 150g --driver-cores 5 --driver-memory 5g  \
#         "bf-cluster/sparksort/node-scripts/target/scala-2.11/sparksort_2.11-0.1.jar" yarn 100000 100 5000