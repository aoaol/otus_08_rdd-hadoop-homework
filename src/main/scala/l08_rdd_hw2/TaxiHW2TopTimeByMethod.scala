package l08_rdd_hw2

import java.sql.Timestamp

import model.TaxiRide
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.rdd.RDD

// https://gist.github.com/aoaol/bbe2820d7e0831279a6ccf96b76ded37
object TaxiHW2TopTimeByMethod extends App {

  case class BestHourData( dayHour: Int, pickups: Int) {
    def toStr(): String = s"$dayHour\t\t$pickups"
  }

  // Getting data to RDD.
  def loadParquet2RDD( path: String)(implicit spark: SparkSession): RDD[ TaxiRide ] = {
    import spark.implicits._
    spark.read.load( path ).as[ TaxiRide ].rdd
  }

  // Counting number of pickups by a hour of day.
  def taxiPickupsByHour( rddTaxi: RDD[ TaxiRide ]): RDD[ BestHourData ] = {
    // x.getTimestamp(1)  is "pickup_datetime" column
    rddTaxi.map( x => (x.tpep_pickup_datetime.toLocalDateTime.getHour, 1) )
      .reduceByKey(_ + _)
      .sortBy( _._2, ascending = false)
      .map( x => BestHourData( x._1, x._2))
  }

  // Saving the result to a file.
  def saveRDD2TextFile( listGoal: RDD[ BestHourData ], outDir: String): Unit = {
    import java.nio.file.{ Files, Paths }
    import scala.reflect.io.Directory
    import java.io.File

    // Deleting target because saveAsTextFile() can not overwrite it.
    if (Files.exists( Paths.get( outDir ))) {
      val directory = new Directory( new File( outDir ))
      directory.deleteRecursively()
    }

    listGoal.map( x => x.toStr()).coalesce(1).saveAsTextFile( outDir )
  }

  override def main ( args: Array[ String ] ): Unit = {
    val parquetDir = "src/main/resources/data/yellow_taxi_jan_25_2018"
    val outDir = "src/main/resources/data_out/top_times"
    implicit val spark = SparkSession.builder().config("spark.master", "local")
      .appName("Taxi - Top Time Application")
      .getOrCreate()

    val rddTaxiF = loadParquet2RDD( parquetDir )

    val listGoal = taxiPickupsByHour( rddTaxiF )

    listGoal.persist()

    println("Top 10 hours:")
    listGoal.take(10).foreach( x => println( x.toStr()))

    try {
      saveRDD2TextFile( listGoal, outDir)
    }
    finally {
      listGoal.unpersist()
    }
  }

}
