# PowerMeasurement
This repo contains scripts used to collect and analyze power measurements of sort application running on a spark cluster.

The repo is organized as follows:

  * [`v1`](v1): includes the scripts from first iteration, spark sort written in scala and bash scripts to run it on the cluster and collect power readings and cpu, memory, disk usage using sar
  * [`v2`](v2): includes more recent scripts to streamline the process of running experiments and analysing results
    * [`python-scripts`](v2/python-scripts): includes python files to run the experiments, and to parse and plot the results
    * [`node-scripts`](v2/node-scripts): includes bash scripts that are copied to all the nodes to help with setting and cleaning up enviroment for experiments
    * [`spark-sort`](v2/spark-sort): includes scala project for different sort implementations run on the cluster 
