package l08_rdd_hw2

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import scala.reflect.io.Directory
import java.io.File

// https://gist.github.com/aoaol/bbe2820d7e0831279a6ccf96b76ded37
object TaxiHW1BeTopTime extends App {

  val outDir = "src/main/resources/data_out/top_times"
  val spark = SparkSession.builder().config("spark.master", "local")
    .appName("Taxi - Top Time Application")
    .getOrCreate()

  val rddTaxiF = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018").rdd

  // x.getTimestamp(1)  is "pickup_datetime" column
  val listGoal = rddTaxiF.map( x => (x.getTimestamp(1).toLocalDateTime.getHour, 1) )
    .reduceByKey(_ + _)
    .sortBy( _._2, ascending = false)
    .map( x => s"${x._1}\t\t${x._2}")

  listGoal.persist()

  println("Top 10 hours:")
  listGoal.take(10).foreach( x => println( x ))

  // Deleting target because saveAsTextFile() can not overwrite it.
  if (Files.exists( Paths.get( outDir ))) {
    val directory = new Directory( new File(outDir))
    directory.deleteRecursively()
  }

  listGoal.coalesce(1).saveAsTextFile(outDir)

  listGoal.unpersist()
}
