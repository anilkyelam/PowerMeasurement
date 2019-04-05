SRC_DIR_FULL_PATH=$1
RESULTS_DIR_FULL_PATH=$2
GIRAPH_CLASS_NAME=$3
INPUT_GRAPH_NAME=$4

# Run giraph job
source ~/.profile
giraph /usr/local/home/hadoop/giraph/giraph-examples/target/giraph-examples-1.3.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar \
        org.apache.giraph.examples.SimplePageRankComputation -vif org.apache.giraph.io.formats.LongDoubleFloatTextInputFormat -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
        -vip /user/ayelam/graph_inputs/${INPUT_GRAPH_NAME}  -op /tmp/giraph_outputs/${INPUT_GRAPH_NAME} -w 7 2>&1
