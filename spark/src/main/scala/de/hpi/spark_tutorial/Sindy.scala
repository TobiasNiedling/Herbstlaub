package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.{collect_set, size}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.commons.cli._

object Sindy {

  /**
  * Parses arguments from command line
  **/
  private def getCommands(args: Array[String]): (String, String) = {
    val options: Options = new Options()
    options.addOption("path", true, "input path of tpch files")
    options.addOption("cores", true, "number of cores to be used")

    val parser: CommandLineParser = new BasicParser()
    val cmd: CommandLine = parser.parse(options, args)

    val cores_ = cmd.getOptionValue("cores")
    val path_ = cmd.getOptionValue("path")

    val cores = if(cores_ == null) "TPCH" else cores_
    val path = if(path_ == null) "4" else path_
  }

  def main(args: Array[String]): Unit = {

    val (cores, path) = getCommands(args)

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark Setup
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$cores]") 
    implicit val spark = sparkBuilder.getOrCreate()
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    // Lets go
    List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      //Read all files
      .map{
        path =>
        val input = spark.read
          .option("header", "true")
          .option("delimiter", ";")
          .csv(s"$path/tpch_$path.csv")   
        val columns = input.columns //input im map aufrufen findet spark eher unwitzig
        input
          .flatMap(row => row.toSeq.zipWithIndex
            .map{case (cell, index) => (cell.toString, columns(index))} //as suggested in slides: build key value pairs of cell value and column name
          )
          .distinct 
      }
      .reduce(_ union _) //get one DF instead of List[Dataset]
      .toDF("key", "value")
      .groupBy("key")
      .agg(collect_set("value")) //get all columns containing that value
      .select("collect_set(value)") //column values are not needed any longer
      .as[Array[String]]
      .flatMap(
        row => row.map(
          col => (col, row.filter(!_.equals(col)).map(other => other)) //each column is obviously contained in it self, so remove it with filter
        )
      ) //create a tuple for each column that may be contained in others
      .toDF("included", "in")
      .distinct
      .groupBy("included")
      .agg(collect_set("in")) //collect all possible inclusions for that column
      .as[(String, Array[Array[String]])]
      .map(
        row => (row._1, row._2.reduce((a,b) => a intersect b))
      ) //intersection between all arrays is the correct inclusion
      .toDF("included", "in")
      .where(size($"in")>0) //prepare for output
      .orderBy("included")
      .as[(String, Array[String])] 
      .collect.foreach(
        row => println(s"${row._1} < ${row._2.mkString(", ")}")
      )
  }
}
