echo "Deleting previous outputs saved on hdfs"
hdfs dfs -rm -r -f -skipTrash /user/ayelam/sort_outputs/*

# Kill any leftover processes from a previous operation
pkill sar
# pkill python3

# Get inputs and validate them
SRC_DIR_FULL_PATH=$1
SIZE_IN_MB=$2
CACHE_HDFS_FILE=$3

# Validate inputs
if [ -z "$SRC_DIR_FULL_PATH" ]
then
        echo "Please provide directory of source/script files"
        exit -1
fi
if [ -z "$SIZE_IN_MB" ]
then
        echo "Please provide required size of input file in MB"
        exit -1
fi
if [ -z "$CACHE_HDFS_FILE" ]
then
        echo "Please provide CACHE_HDFS_FILE parameter"
        exit -1
fi

# Adding file to hdfs
echo "Checking if required input file exists"
FOLDER_PATH_HDFS="/user/ayelam/sort_inputs/${SIZE_IN_MB}mb"
hdfs dfs -test -e ${FOLDER_PATH_HDFS}
if [ $? != 0 ]; then
        echo "Input folder not found, creating it"
        hdfs dfs -mkdir ${FOLDER_PATH_HDFS}

        # We will generate input file in parts of 20GB pieces. Only last piece has size less than 20GB.
        # Each part has (20 * 1000MB * 1000KB * 10) 100MB records which makes it 10^9B each.
        RECORDS_PER_MB=$((1000*10))
	NUM_PARTS=$((SIZE_IN_MB/20000))
	NORMAL_PART_SIZE=$((20*1000*RECORDS_PER_MB))
        LAST_PART_SIZE=$(((SIZE_IN_MB%20000)*RECORDS_PER_MB))
	echo ${NORMAL_PART_SIZE} ${LAST_PART_SIZE}
        for (( i=0; i<=NUM_PARTS; i++ ))
        do  
                FILE_PATH_LOCAL="/usr/local/home/ayelam/part_${i}_${SIZE_IN_MB}mb.input"
                STARTING_RECORD=$((i*PART_SIZE))
                if [ "$i" -eq "$NUM_PARTS" ]; then PART_SIZE="${LAST_PART_SIZE}"; else PART_SIZE="${NORMAL_PART_SIZE}"; fi
                echo "Generating ${PART_SIZE} records for part $i of input at ${FILE_PATH_LOCAL}"
                ${SRC_DIR_FULL_PATH}/gensort -t20 -b${STARTING_RECORD} ${PART_SIZE} ${FILE_PATH_LOCAL}

                # Copy over to hdfs
                hdfs dfs -D dfs.replication=2 -put ${FILE_PATH_LOCAL} ${FOLDER_PATH_HDFS}
                hdfs dfs -setrep 1 ${FOLDER_PATH_HDFS}

                # Remove local file
                rm ${FILE_PATH_LOCAL}
        done

        # Test again
        hdfs dfs -du -h ${FOLDER_PATH_HDFS}
        hdfs dfs -test -e ${FOLDER_PATH_HDFS}
        if [ $? != 0 ]; then
                        echo "Could not create input file for spark sort, check errors"
                        exit -1
        fi
else
        echo "Input folder ${FOLDER_PATH_HDFS} exists, moving on."
fi

# Adding file to hdfs cache
if [ "${CACHE_HDFS_FILE}" == 1 ]; then
	echo "Checking if the hdfs file is cached"
	hdfs cacheadmin -listDirectives | grep "${FOLDER_PATH_HDFS}"
	if [ $? != 0 ]; then
		echo "File not in cache, refreshing the cache and adding the file"
		hdfs cacheadmin -removeDirectives -path "/user/ayelam"
		hdfs cacheadmin -addDirective -path "${FOLDER_PATH_HDFS}" -pool cache-pool
		
		# This initiates caching process. Ideally we would want to wait and check until it finishes, but for now
		# we just wait and hope that it's done. (TODO: Configure this time for larger inputs)
		sleep 60
	
		hdfs cacheadmin -listDirectives
                hdfs cacheadmin -listPools cache-pool -stats
	else
		echo "File is cached, moving on."
	fi
fi
