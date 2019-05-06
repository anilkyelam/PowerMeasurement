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
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer

import scala.collection.mutable.ArrayBuffer

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
      println("Requires two arguments, SparkMaster and Input size or file path")
      System.exit(0)
    }

    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("TeraSort")
    val sc = new SparkContext(conf)

    // Processing input from command line arguments
    var input_path:String = null
    try {
      val input_size_gb = args(1).toInt
      if (input_size_gb > 200 || input_size_gb % 20 != 0){
        println("If specifying input size in gb, provide a multiple of 20gb and make sure it is not above 200gb!")
        System.exit(-1)
      }

      val num_parts = input_size_gb / 20
      var i : Int = 0
      input_path = "/user/ayelam/sort_inputs/200000mb/part_{0}_200000mb.input".format(i)
      while (i < num_parts-1){
        i += 1
        input_path += ",/user/ayelam/sort_inputs/200000mb/part_{0}_200000mb.input".format(i)
      }
    } catch {
      // If argument is not a number, consider it a path to input file or folder
      case e: NumberFormatException => input_path = args(0)
    }
    println("Loading input files from path(s): " + input_path)

    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](input_path)
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
