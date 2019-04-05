echo "Deleting previous outputs saved on hdfs"
hdfs dfs -rm -r -f -skipTrash /user/ayelam/graph_outputs/*
hdfs dfs -rm -r -f -skipTrash /tmp/giraph_outputs/*

# Kill any leftover processes from a previous operation
pkill sar
# pkill python3

