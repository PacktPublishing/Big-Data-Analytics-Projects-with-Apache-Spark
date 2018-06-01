package com.tomekl007.mba

import com.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object FindAssociationRules extends Logging{

  def main(args: Array[String]): Unit = {

    if (args.size < 2) {
      println("Usage: FindAssociationRules <input-path> <output-path>")
      sys.exit(1)
    }

    val sc = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

    val input = args(0)
    val output = args(1)

    val transactions = sc.textFile(input)

    val assocRules: RDD[(List[String], List[String], Double)]
    = performMBAAnalysis(transactions)

    // Formatting the result just for easy reading.
    val formatResult = assocRules.map(f => {
      (f._1.mkString("[", ",", "]"), f._2.mkString("[", ",", "]"), f._3)
    })
    formatResult.saveAsTextFile(output)

    // done!
    sc.stop()
  }


  def findCountsOfOccurringSequences(transactions: RDD[String]): RDD[(List[String], Int)] = {
    val patterns = transactions.flatMap(line => {
      val items = line.split(",").toList
      (0 to items.size) flatMap items.combinations filter (xs => xs.nonEmpty)
    }).map((_, 1))

    log.info(s"patterns: ${patterns.collect().toList}")

    val combined = patterns.reduceByKey(_ + _)

    log.info(s"combined: ${combined.collect().toList}")
    combined
  }

  def performMBAAnalysis(transactions: RDD[String]) = {


    val combined: RDD[(List[String], Int)] = findCountsOfOccurringSequences(transactions)

    val subpatterns = combined.flatMap(pattern => {
      val result = ListBuffer.empty[(List[String], (List[String], Int))]
      result += ((pattern._1, (Nil, pattern._2)))

      val sublist = for {
        i <- pattern._1.indices
        xs = pattern._1.take(i) ++ pattern._1.drop(i + 1)
        if xs.nonEmpty
      } yield (xs, (pattern._1, pattern._2))
      result ++= sublist
      result.toList
    })

    log.info(s"subpatterns: ${subpatterns.collect().toList}")
    val rules = subpatterns.groupByKey()

    val assocRules = rules.map(in => {
      val fromCount = in._2.find(p => p._1 == Nil).get
      val toList = in._2.filter(p => p._1 != Nil).toList
      if (toList.isEmpty) Nil
      else {
        val result =
          for {
            t2 <- toList
            confidence = t2._2.toDouble / fromCount._2.toDouble
            difference = t2._1 diff in._1
          } yield (in._1, difference, confidence)
        result
      }
    })
    assocRules.flatMap(identity)
  }
}