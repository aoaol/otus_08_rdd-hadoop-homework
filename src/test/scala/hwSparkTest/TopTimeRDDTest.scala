package hwSparkTest

import l08_rdd_hw2.TaxiHW2TopTimeByMethod.{ BestHourData, loadParquet2RDD, taxiPickupsByHour }
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TopTimeRDDTest extends AnyFlatSpec with Matchers {
  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for Big Data Application")
    .getOrCreate()


  it should "Successfully calculate the distribution of number of pickups by a hour of day" in {
    val rddTaxi = loadParquet2RDD("src/main/resources/data/yellow_taxi_jan_25_2018")

    val bestHourPickUps = taxiPickupsByHour( rddTaxi ).take(2).toSeq
    val answer = Array( BestHourData( 19, 22121),
                        BestHourData( 20, 21598))
                        .toSeq

    bestHourPickUps should equal (answer)
  }
}
