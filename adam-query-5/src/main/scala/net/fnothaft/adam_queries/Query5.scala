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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{ LabeledPoint, LinearRegressionWithSGD }
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
import org.bdgenomics.formats.avro.AlignmentRecord
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import scala.math.{ exp, log, min }

object Query5 extends ADAMCommandCompanion {
  val commandName = "query5"
  val commandDescription = ""

  def apply(cmdLine: Array[String]) = {
    new Query5(Args4j[Query5Args](cmdLine))
  }
}

class Query5Args extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM read file to apply the query to", index = 0)
  var inputPath1: String = null
}

object Query5Helper extends Serializable {
  def idx(base: Char): Int = base match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
  }

  def dinucToIdx(context: String): Int = {
    4 * idx(context(0)) + idx(context(1))
  }

  def kmerToDinucFeature(kmer: String, multiplicity: Long): Option[LabeledPoint] = {
    // slice kmers into contexts
    val contexts = kmer.sliding(2).toIterable

    if (contexts.size > 0) {
      val ctxReciprocal = 1.0 / contexts.size.toDouble

      // populate initial context array
      val contextCounts = Array.fill[Double](16) { 0.0 }

      // update counts
      contexts.foreach(c => contextCounts(dinucToIdx(c)) += ctxReciprocal)

      Some(new LabeledPoint(log(multiplicity.toDouble), Vectors.dense(contextCounts)))
    } else {
      None
    }
  }
}

class Query5(protected val args: Query5Args) extends ADAMSparkCommand[Query5Args] with Logging {
  val companion = Query5

  def run(sc: SparkContext, job: Job) {
    val projection = Projection(AlignmentRecordField.sequence)
    val reads1 = sc.loadAlignments(args.inputPath1, projection = Some(projection))
      .cache()
    println("have " + reads1.count() + " reads")

    val counts1 = reads1
      .adamCountKmers(20)
      .filter(kv => {
        kv._2 < 200 && kv._2 > 1 &&
          kv._1.forall(c => c == 'A' || c == 'C' || c == 'G' || c == 'T')
      }).flatMap(kv => Query5Helper.kmerToDinucFeature(kv._1, kv._2))
      .cache()
    reads1.unpersist()

    val model = new LinearRegressionWithSGD().setIntercept(true).run(counts1)

    println("Model has intercept:")
    println(model.intercept)
    println("Model has coefficients:")
    model.weights.toArray.foreach(println)
  }
}
