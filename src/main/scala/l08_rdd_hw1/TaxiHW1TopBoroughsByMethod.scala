package l08_rdd_hw1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

// https://gist.github.com/aoaol/2157bc095b88c58431661fa0a9ba35b0
object TaxiHW1TopBoroughsByMethod extends App {


  // Getting data to RDD.
  def loadParquet2DF( path: String) (implicit spark: SparkSession): DataFrame = spark.read.load( path )

  // Loading csv data to Dataframe
  def readCSV( path: String) (implicit spark: SparkSession): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv( path )

  // Counting number of pickups by borough.
  def taxiPickupsByBorough( dfTaxi: DataFrame, dfTaxiZones: DataFrame) (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    dfTaxi.join( broadcast( dfTaxiZones ), $"DOLocationID" === $"LocationID", "left")
        .groupBy($"Borough")
        .count()
        .orderBy($"count".desc)
  }

  def writeDF2Parquet( df2Write: DataFrame, dir: String) : Unit =
    df2Write.repartition(1).write.mode("overwrite").parquet( dir )



  override def main ( args: Array[ String ] ): Unit = {
    val parquetDir = "src/main/resources/data/yellow_taxi_jan_25_2018"
    val csvFile = "src/main/resources/data/taxi_zones.csv"
    val outDir = "src/main/resources/data_out/hw1_top_boroughs.parquet"
    implicit val spark = SparkSession.builder().config("spark.master", "local")
      .appName("Taxi - Top Boroughs Application")
      .getOrCreate()


    val dfTaxiF = loadParquet2DF( parquetDir )
    val dfTaxiZones = readCSV( csvFile )
    val dfGoal = taxiPickupsByBorough( dfTaxiF, dfTaxiZones )

    dfGoal.cache()
    dfGoal.show()
    try {
      writeDF2Parquet( dfGoal, outDir )
    }
    finally {
      dfGoal.unpersist()
    }

    dfGoal.unpersist()
  }
}
