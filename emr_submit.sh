#aws emr create-cluster --release-label emr-5.9.0 --applications Name=Spark --ec2-attributes KeyName=myKey --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.large --auto-terminate
#spark-submit --master yarn --num-executors 100 --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13 --class ch.cern.testSparkMeasure.testSparkMeasure target/scala-2.11/testsparkmeasurescala_2.11-0.1.jar

#https://aws.amazon.com/ec2/previous-generation/
# m5.2xlarge, 32 GB Memory, 

### https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html
### (1) hdfs:// (or no prefix)
###     HDFS is used by the master and core nodes. One advantage is that it's fast; a disadvantage is that it's ephemeral storage which is reclaimed when the cluster ends.
### (2) s3://	
###		EMRFS is an implementation of the Hadoop file system used for reading and writing regular files from Amazon EMR directly to Amazon S3.
###     It can be used with "--emrfs" option to a "aws emr create-cluster Arg1...ArgN" command

#  --emrfs \
#  --auto-terminate \
#  --instance-type m3.2xlarge --instance-count 4 \
## 16 vCore, 64 GiB memory, EBS only storage
## default EBS Storage:32 GiB
## EBS: Iops value maximum is sizeInGB * 50).

###EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=100}},{VolumeSpecification={VolumeType=io1,SizeInGB=100,Iops=100},VolumesPerInstance=4}]}
#--instance-groups '[{"InstanceCount":6,"InstanceGroupType":"CORE","InstanceType":"m5.4xlarge","Name":"Slave-6","EbsConfiguration":{ "EbsOptimized": true,"EbsBlockDeviceConfigs":[{ "VolumeSpecification": { "VolumeType": "io1","SizeInGB": 200},"VolumesPerInstance": 1}]}},
## {"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m5.4xlarge","Name":"Master","EbsConfiguration":{"EbsOptimized": true,"EbsBlockDeviceConfigs":[{ "VolumeSpecification": { "VolumeType": "io1","SizeInGB": 300},"VolumesPerInstance": 1}]}}]' \
#"EbsConfiguration":{"EbsOptimized": true, "EbsBlockDeviceConfigs": [{ "VolumeSpecification": { "VolumeType": "io1","SizeInGB": 200}, "VolumesPerInstance": integer}]},

aws emr create-cluster --name "sort256gb-ganglia-test" \
  --release-label emr-5.17.0 \
  --enable-debugging \
  --ebs-root-volume-size 10 \
  --no-auto-terminate \
  --ec2-attributes '{"KeyName":"shw328_key0","InstanceProfile":"EMREC2","SubnetId":"subnet-474bc31e"}' \
  --instance-groups 'InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.4xlarge,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=io1,SizeInGB=200,Iops=10000},VolumesPerInstance=1}]}' 'InstanceGroupType=CORE,InstanceCount=6,InstanceType=m5.4xlarge,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=io1,SizeInGB=200,Iops=10000},VolumesPerInstance=1}]}' \
  --service-role emrrole \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --log-uri s3://stingw-sortlog/`date +%Y%m%d%H%M%S`/logs \
  --applications Name=Hadoop Name=Spark Name=Ganglia
  --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--num-executors","17","--executor-cores","5","--executor-memory","18G","--class","edu.uscd.sysnet.sort.testSparkMeasure","s3://stingw-sortinput/sortbytesscala_2.11-1.2.0.jar","s3://stingw-sortinput/256gb5.input", "s3://stingw-sortoutput/output_256gb_0"],
  "Type":"CUSTOM_JAR","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar","Properties":"","Name":"SparkSort"}]'

  #--steps Type=Spark,Name="SparkSort",Args=[--deploy-mode,cluster,--num-executors,10,--master,yarn,--class,edu.uscd.sysnet.sort.testSparkMeasure,s3://stingw-sortinput/sortbytesscala_2.11-1.2.0.jar,s3://stingw-sortinput/10gb.input, s3://stingw-sortoutput/output_10gb]
  #--steps "Name="SparkSort",Type=Spark,Args=[--deploy-mode,cluster,--num-executors,10,--master,yarn,--packages,ch.cern.sparkmeasure:spark-measure_2.11:0.13,--class,ch.cern.testSparkMeasure.testSparkMeasure,s3://stingw-sortinput/testsparkmeasurescala_2.11-0.1.jar]"