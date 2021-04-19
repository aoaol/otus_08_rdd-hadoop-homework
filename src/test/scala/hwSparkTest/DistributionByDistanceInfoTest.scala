package hwSparkTest

import l08_rdd_hw3.TaxiHW3DistanceInfoByMethod.{ loadParquet2DF, taxiPickupsByDistanceRange, writeDF2Postgres }
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession


class DistributionByDistanceInfoTest extends SharedSparkSession {

  test("join - processTaxiData") {
    val taxiDF = loadParquet2DF("src/main/resources/data/yellow_taxi_jan_25_2018")

    val distanceDistribution = taxiPickupsByDistanceRange( taxiDF )

    checkAnswer( distanceDistribution.limit( 2 ),
      Row( 0.0, 15088, 0.01, 0.38, 2.88, 5.8, 0.72, 2.01, 0.25)  ::
      Row( 0.1, 35616, 0.48, 0.6,  4.73, 6.96, 1.0, 1.47, 0.21) ::  Nil
    )
  }
}
