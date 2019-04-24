/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package PowerMeasurements

import java.util.Comparator
import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object TeraSort {

  implicit val caseInsensitiveOrdering : Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage:")
      println("DRIVER_MEMORY=[mem] spark-submit " +
        "com.github.ehiggs.spark.terasort.TeraSort " +
        "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
        "[input-file]")
      println(" ")
      println("Example:")
      println("DRIVER_MEMORY=50g spark-submit " +
        "com.github.ehiggs.spark.terasort.TeraSort " +
        "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
        "/home/myuser/terasort_in")
      System.exit(0)
    }

    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("TeraSort")
    val sc = new SparkContext(conf)

    // Process command line arguments
    val inputFile = args(1)
    // val outputFile = args(1)

    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
    dataset.setName("InputRDD")
    // dataset.persist()
    // dataset.mapPartitions(iter => Array(iter.size).iterator, true).collect().foreach(println)

    val sorted = dataset.repartitionAndSortWithinPartitions(new TeraSortPartitioner(dataset.partitions.length))
    sorted.setName("SortedRDD")
    // sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    // sorted.persist()

    var count = sorted.count()
    println("Terasort output records count: " + count)

    // Get sizes of sorted partitions
    // sorted.mapPartitions(iter => Array(iter.size).iterator, true).collect().foreach(println)

    // To keep the spark job alive for checking things on Web UI
    // Thread.sleep(60000)

    sc.stop()
  }
}
