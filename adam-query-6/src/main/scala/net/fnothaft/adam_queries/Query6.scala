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
import org.bdgenomics.adam.projections.{ Projection, AlignmentRecordField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.AlignmentRecordContext._
import org.bdgenomics.adam.rdd.variation.VariationContext._
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.{ AlignmentRecord, Genotype }
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import scala.math.{ exp, log, min }

object Query6 extends ADAMCommandCompanion {
  val commandName = "query6"
  val commandDescription = ""

  def apply(cmdLine: Array[String]) = {
    new Query6(Args4j[Query6Args](cmdLine))
  }
}

class Query6Args extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM read file to apply the query to", index = 0)
  var inputPath1: String = null
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM genotype file to apply the query to", index = 0)
  var inputPath2: String = null
}

class Query6(protected val args: Query6Args) extends ADAMSparkCommand[Query6Args] with Logging {
  val companion = Query6

  def run(sc: SparkContext, job: Job) {
    val reads = sc.loadAlignments(args.inputPath1)

    val gts: RDD[Genotype] = sc.adamLoad(args.inputPath2)
    val variants = gts.map(g => new RichVariant(g.getVariant))
    val knownSnps = SnpTable(variants)
    val recalled = reads.adamBQSR(sc.broadcast(knownSnps)).cache()
    println("Recalibrated " + recalled.count() + " reads.")
  }
}
