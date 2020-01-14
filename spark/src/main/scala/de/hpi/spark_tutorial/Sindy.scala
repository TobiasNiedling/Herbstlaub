package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Sindy {

  def main(args: Array[String]): Unit = {

    //////////////////////////////////////////////
    /////// SET UP ///////////////////////////////
    //////////////////////////////////////////////

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    //////////////////////////////////////////////
    /////// PROCESS //////////////////////////////
    //////////////////////////////////////////////

    val input = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("data/tpch_nation.csv")
      
    val columns = input.columns //df im map aufrufen findet spark eher unwitzig
    input.map(row => row.toSeq.zipWithIndex.map{case (cell, index) => (cell.toString, columns(index))}).show(false) //a suggested in slides: build key value pairs of cell value and column name
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO
  }
}
