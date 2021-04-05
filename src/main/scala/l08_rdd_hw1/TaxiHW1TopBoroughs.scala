package l08_rdd_hw1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

// https://gist.github.com/aoaol/2157bc095b88c58431661fa0a9ba35b0
object TaxiHW1BeTopBoroughs extends App {

  val spark = SparkSession.builder().config("spark.master", "local")
    .appName("Taxi - Top Boroughs Application")
    .getOrCreate()

  import spark.implicits._

  val dfTaxiF: DataFrame = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  val dfTaxiZones: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema","true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val dfGoal = dfTaxiF.join( broadcast( dfTaxiZones ), $"DOLocationID" === $"LocationID", "left")
    .groupBy($"Borough")
    .count()
    .orderBy($"count".desc)

  dfGoal.cache()

  dfGoal.repartition(1).write.mode("overwrite").parquet("src/main/resources/data_out/hw1_top_boroughs.parquet")
  dfGoal.show()

  dfGoal.unpersist()
}
