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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.VariationContext._
import org.bdgenomics.formats.avro.Genotype
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Query3 extends ADAMCommandCompanion {
  val commandName = "query3"
  val commandDescription = ""

  def apply(cmdLine: Array[String]) = {
    new Query3(Args4j[Query3Args](cmdLine))
  }
}

class Query3Args extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT1", usage = "The ADAM variant file to apply the query to", index = 0)
  var inputPath1: String = null
  @Argument(required = true, metaVar = "INPUT2", usage = "The ADAM variant file to apply the query to", index = 1)
  var inputPath2: String = null
}

class Query3(protected val args: Query3Args) extends ADAMSparkCommand[Query3Args] with Logging {
  val companion = Query3

  def run(sc: SparkContext, job: Job) {
    val gt1: RDD[Genotype] = sc.adamLoad(args.inputPath1)
    val gt2: RDD[Genotype] = sc.adamLoad(args.inputPath2)

    val concordance = gt1.concordanceWith(gt2)
      .collect()

    println(concordance)
  }
}
