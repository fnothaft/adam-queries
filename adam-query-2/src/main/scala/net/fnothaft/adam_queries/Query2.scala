/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.adam_queries

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus._
import org.bdgenomics.adam.cli._
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.AlignmentRecordContext._
import org.bdgenomics.adam.rdd.variation.VariationContext._
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.AlignmentRecord
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import scala.math.min

object Query2 extends ADAMCommandCompanion {
  val commandName = "transform"
  val commandDescription = "Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations"

  def apply(cmdLine: Array[String]) = {
    new Query2(Args4j[Query2Args](cmdLine))
  }
}

class Query2Args extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT1", usage = "The ADAM, BAM or SAM file to apply the query to", index = 0)
  var inputPath1: String = null
  @Argument(required = true, metaVar = "INPUT2", usage = "The ADAM, BAM or SAM file to apply the query to", index = 1)
  var inputPath2: String = null
}

class Query2(protected val args: Query2Args) extends ADAMSparkCommand[Query2Args] with Logging {
  val companion = Query2

  def run(sc: SparkContext, job: Job) {
    val counts1 = sc.loadAlignments(args.inputPath1)
      .adamCharacterizeTags()
      .cache()
    val total1 = counts1.count
    val unique1 = counts1.map(kv => kv._2).reduce(_ + _)

    val counts2 = sc.loadAlignments(args.inputPath2)
      .adamCharacterizeTags()
      .cache()
    val total2 = counts2.count
    val unique2 = counts2.map(kv => kv._2).reduce(_ + _)

    val joined = counts1.join(counts2)
      .cache()
    counts1.unpersist()
    counts2.unpersist()
    val total = joined.count
    val matching = joined.map(kv => {
      val (_, (v1, v2)) = kv
      min(v1, v2)
    }).reduce(_ + _)

    println("Sample\tUnique\tTotal")
    println("1\t%d\t%d".format(total1, unique1))
    println("2\t%d\t%d".format(total2, unique2))
    println("Total\t%d\t%d".format(total, matching))
  }
}
