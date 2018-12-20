pkill python3
pkill sar
DURATION=$1
SIZE=$2
TC=$3
PERIOD=1 # 1 sec period for power metering
DIR=$(hostname | cut -d'.' -f1)
unixstamp=$(date +%s)
echo "the host is:"${DIR}
mkdir ${DIR}_${SIZE}gb_${unixstamp}
if [ ${DIR} = "ccied21" ]; then
### delete the previous output directory
echo "HDFS tries to delete previous output dirs"
#hdfs dfs -rm -r /user/shw328/10gb_output
#hdfs dfs -rm -r /user/shw328/.Trash/Current/user/shw328/10gb_output
hdfs dfs -rm -r /user/shw328/128gb_output
hdfs dfs -rm -r /user/shw328/.Trash/Current/user/shw328/128gb_output
#hdfs dfs -rm -r /user/shw328/256gb_output
#hdfs dfs -rm -r /user/shw328/.Trash/Current/user/shw328/256gb_output	
#hdfs dfs -rm -r /user/shw328/512gb_output
#hdfs dfs -rm -r /user/shw328/.Trash/Current/user/shw328/512gb_output
echo "the powermeter connects to ccied21"
#nohup python3 meter_reading.py ${unixstamp}_tc ${DURATION} 2>&1 & #with tc
nohup python3 meter_reading.py ${unixstamp}_${TC} ${DURATION} ${PERIOD} 2>&1 &
fi 
nohup sar -rb 1 ${DURATION} > ${DIR}_${SIZE}gb_${unixstamp}/memio_$DIR.sar 2>&1 &
nohup sar -P ALL 1 ${DURATION} > ${DIR}_${SIZE}gb_${unixstamp}/cpu_$DIR.sar 2>&1 &
nohup sar -n DEV 1 ${DURATION} > ${DIR}_${SIZE}gb_${unixstamp}/net_$DIR.sar 2>&1 &

if [ ${DIR} = "ccied21" ]; then
echo "spark2-submit from master node ccied21"
nohup spark2-submit --master yarn --num-executors 40 --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13 \
--class edu.uscd.sysnet.sort.sortBytesScala target/scala-2.11/sortbytesscala_2.11-1.2.0.jar \
/user/shw328/${SIZE}gb.input /user/shw328/${SIZE}gb_output /user/shw328/taskstats_${unixstamp} > spark_${SIZE}gb_${unixstamp}_${TC}.log 2>&1 &
fi

### submit the job
#spark2-submit --master yarn --num-executors 1000 --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13 \
#--class edu.uscd.sysnet.sort.sortBytesScala target/scala-2.11/sortbytesscala_2.11-1.2.0.jar \
#/user/shw328/10gb.input /user/shw328/10gb_output /user/shw328/taskstats_${unixstamp} \

#spark2-submit --master yarn --num-executors 1900 --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13 \
#--class edu.uscd.sysnet.sort.sortBytesScala target/scala-2.11/sortbytesscala_2.11-1.2.0.jar \
#/user/shw328/256gb5.input /user/shw328/256gb5_output /user/shw328/taskstats_${unixstamp} \

### Move the log file to local
#hdfs dfs -copyToLocal /user/shw328/taskstats_${unixstamp} /home/shw328

