# PowerMeasurement
This repo contains scripts used to collect and analyze power measurements of sort application running on a spark cluster.

The repo is organized as follows:

  * [`v1`](v1): includes the scripts from first iteration, spark sort written in scala and bash scripts to run it on the cluster and collect power readings and cpu, memory, disk usage using sar
  * [`v2`](v2): includes more recent scripts to streamline the process of running experiments and analysing results
    * [`test_cluster`](v2/test_cluster): includes python scripts to run the experiments, and to parse and plot the results on the test hadoop cluster (ccied machiens)
    * [`bf_cluster`](v2/bf_cluster): includes similar scripts (as for test_cluster) targetting the b09 machines connected to the barefoot P4 switch
    * [`spark-sort`](v2/spark-sort): includes scala project for different sort implementations we run on the cluster 
