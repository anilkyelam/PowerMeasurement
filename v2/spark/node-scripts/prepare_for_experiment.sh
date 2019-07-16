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
FOLDER_PATH_HDFS="/user/ayelam/sort_inputs"
hdfs dfs -test -e ${FOLDER_PATH_HDFS}
if [ $? != 0 ]; then
        echo "Sort input folder not found, error!"
        exit -1
else
        echo "Sort input folder ${FOLDER_PATH_HDFS} exists, moving on."
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
		sleep 180
	
		hdfs cacheadmin -listDirectives
                hdfs cacheadmin -listPools cache-pool -stats
	else
		echo "File is cached, moving on."
	fi
fi
