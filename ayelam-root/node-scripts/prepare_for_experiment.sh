echo "Deleting previous outputs saved on hdfs"
hdfs dfs -rm -r -f /user/ayelam/sort_outputs/*


# Get inputs and validate them
SRC_DIR_FULL_PATH=$1
INPUT_SIZE_MB=$2

if [ -z "$SRC_DIR_FULL_PATH" ]
then
        echo "Please provide directory of source/script files"
        exit -1
fi
if [ -z "$INPUT_SIZE_MB" ]
then
        echo "Please provide required size of input file in MB"
        exit -1
fi

echo "Checking if required input file exists"
FILE_PATH_HDFS="/user/ayelam/sort_inputs/${INPUT_SIZE_MB}mb.input"
hdfs dfs -test -e ${FILE_PATH_HDFS}
if [ $? != 0 ]; then
   echo "Input file not found, creating it".
   FILE_PATH_LOCAL="$(pwd)/${INPUT_SIZE_MB}mb.input"

   # Run gensort to generate sort input. It takes count and produces (count*100B) sized file.
   GEN_SORT_COUNT=$((INPUT_SIZE_MB*10000))
   ${SRC_DIR_FULL_PATH}/gensort -b0 ${GEN_SORT_COUNT} ${FILE_PATH_LOCAL}

   # Copy over to hdfs
   hdfs dfs -put ${FILE_PATH_LOCAL} ${FILE_PATH_HDFS}

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
