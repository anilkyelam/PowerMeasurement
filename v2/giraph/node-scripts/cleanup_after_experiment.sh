echo "Deleting previous outputs saved on hdfs"
hdfs dfs -rm -r -f -skipTrash /user/ayelam/sort_outputs/*
# hdfs dfs -rm -r -f /user/ayelam/.Trash/Current/user/ayelam/sort_outputs/*