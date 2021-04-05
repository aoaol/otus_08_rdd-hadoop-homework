package l08_rdd_hw2

import org.apache.spark.sql.SparkSession

object TaxiHW1BeTopTime extends App {

  val spark = SparkSession.builder().config("spark.master", "local")
    .appName("Taxi - Top Time Application")
    .getOrCreate()

  val rddTaxiF = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018").rdd

  val listGoal = rddTaxiF.map( x => (x.getTimestamp(1).toLocalDateTime.getHour, 1) )
    .reduceByKey(_ + _)
    .sortBy( _._2, ascending = false)
    .take(10).toList

    listGoal.foreach( x => println(s"${x._1}\t\t${x._2}"))

}
