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
FILE_PATH_HDFS="/user/ayelam/sort_inputs/${SIZE_IN_MB}mb.input"
hdfs dfs -test -e ${FILE_PATH_HDFS}
if [ $? != 0 ]; then
   echo "Input file not found, creating it".
   FILE_PATH_LOCAL="$(pwd)/${SIZE_IN_MB}mb.input"

   # Run gensort to generate sort input. It takes count and produces (count*100B) sized file.
   GEN_SORT_COUNT=$((SIZE_IN_MB*10000))
   ${SRC_DIR_FULL_PATH}/gensort -b0 ${GEN_SORT_COUNT} ${FILE_PATH_LOCAL}
   # echo ${FILE_PATH_LOCAL}

   # Copy over to hdfs
   hdfs dfs -D dfs.replication=2  -put ${FILE_PATH_LOCAL} ${FILE_PATH_HDFS}
   hdfs dfs -setrep 1 ${FILE_PATH_HDFS}
   
   # Remove local file
   rm ${FILE_PATH_LOCAL}

   # Test again
   hdfs dfs -test -e ${FILE_PATH_HDFS}
   if [ $? != 0 ]; then
		echo "Could not create input file for spark sort, check errors"
		exit -1
   fi
else
        echo "Input file ${FILE_PATH_HDFS} exists, moving on."
fi

# Adding file to hdfs cache
if [ "${CACHE_HDFS_FILE}" == 1 ]; then
	echo "Checking if the hdfs file is cached"
	hdfs cacheadmin -listDirectives | grep "${FILE_PATH_HDFS}"
	if [ $? != 0 ]; then
		echo "File not in cache, refreshing the cache and adding the file"
		sudo -u hdfs hdfs cacheadmin -removeDirectives -path "/user/ayelam"
		sudo -u hdfs hdfs cacheadmin -addDirective -path "${FILE_PATH_HDFS}" -pool anil-cache-pool
		
		# This initiates caching process. Ideally we would want to wait and check until it finishes, but for now
		# we just wait and hope that it's done. (TODO: Configure this time for larger inputs)
		sleep 300
	
		hdfs cacheadmin -listDirectives
	else
		echo "File is cached, moving on."
	fi
fi
