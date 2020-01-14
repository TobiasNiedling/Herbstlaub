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
    import org.apache.spark.sql.functions.{collect_set, size}

    //////////////////////////////////////////////
    /////// PROCESS //////////////////////////////
    //////////////////////////////////////////////

    List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map{
        path =>
        val input = spark.read
          .option("header", "true")
          .option("delimiter", ";")
          .csv(s"data/tpch_$path.csv")   
        val columns = input.columns //input im map aufrufen findet spark eher unwitzig
        input
          .flatMap(row => row.toSeq.zipWithIndex
            .map{case (cell, index) => (cell.toString, columns(index))} //a suggested in slides: build key value pairs of cell value and column name
          )
          .distinct 
      }
      .reduce(_ union _) //get one DF instead of List[Dataset]
      .toDF("key", "value")
      .groupBy("key")
      .agg(collect_set("value")) //get all columns containing that value
      .withColumnRenamed("collect_set(value)", "values")
      .select("values") //column values are not needed any longer
      .as[Array[String]]
      .flatMap(
        row => row.map(
          col => (col, row.filter(!_.equals(col)).map(other => other)) //each column is obviously contained in it self, so remove it with filter
        )
      ) //create a tuple for each column contained in others
      .toDF("included", "in")
      .distinct
      .groupBy("included")
      .agg(collect_set("in"))
      .withColumnRenamed("collect_set(in)", "in")
      .as[(String, Array[Array[String]])]
      .map(
        row => (row._1, row._2.reduce((a,b) => a intersect b))
      )
      .toDF("included", "in")
      .where(size($"in")>0)
      .orderBy("included")
      .as[(String, Array[String])] 
      .collect
      .foreach(
        row => println(s"${row._1} < ${row._2.mkString(", ")}")
      )
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO
  }
}
