package l08_rdd_hw3

import java.util.Properties

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object TaxiHW3DistanceInfoByMethod extends App {

  // Getting data to RDD.
  def loadParquet2DF( path: String) (implicit spark: SparkSession): DataFrame = spark.read.load( path )

  // Counting number of pickups by borough.
  def taxiPickupsByDistanceRange( dfTaxi: DataFrame) (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val window  = Window.orderBy("trip_distance")

    dfTaxi.select($"tpep_dropoff_datetime", $"tpep_pickup_datetime", $"trip_distance", $"total_amount", $"tip_amount")
        .withColumn("percent_rank", round( percent_rank().over( window ), 1))
        .withColumn("duration_in_min", round( ($"tpep_dropoff_datetime".cast( LongType ) - $"tpep_pickup_datetime".cast( LongType )) / 60, 2))
        .where( $"trip_distance" > lit(0)   &&   $"duration_in_min" > lit(0))
        .groupBy("percent_rank")
        .agg(
          count("trip_distance").as("total trips"),
          min("trip_distance").as("min distance"),
          callUDF("percentile_approx", $"trip_distance", lit(0.5)).as("median distance"),
          callUDF("percentile_approx", $"duration_in_min", lit(0.5)).as("median duration"),
          callUDF("percentile_approx", $"total_amount", lit(0.5)).as("median total amount"),
          callUDF("percentile_approx", $"tip_amount", lit(0.5)).as("median tip amount"),
        )
        .withColumn("dur total amount", round( $"median total amount" / $"median duration", 2))
        .withColumn("dur tip amount", round( $"median tip amount" / $"median duration", 2))
        .orderBy( $"percent_rank")
  }

  def writeDF2Postgres( df2Write: DataFrame) : Unit = {
    val url = "jdbc:postgresql://localhost:5432/otus"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "docker")
    connectionProperties.put("password", "docker")
    connectionProperties.put("driver", "org.postgresql.Driver")

    df2Write.write.mode("overwrite").jdbc( url, "amount_by_distance", connectionProperties)
  }


  override def main ( args: Array[ String ] ): Unit = {
    implicit val spark = SparkSession.builder().config("spark.master", "local")
      .appName("Taxi - Distance Info Application")
      .getOrCreate()

    val dfTaxiF = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val dfGoal = taxiPickupsByDistanceRange( dfTaxiF )
    dfGoal.cache()

    dfGoal.show()

    try {
      writeDF2Postgres( dfGoal )
    }
    finally {
      dfGoal.unpersist()
    }
  }
}
