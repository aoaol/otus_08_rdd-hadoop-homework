package hwSparkTest


import l08_rdd_hw3.TaxiHW3DistanceInfoByMethod.{ loadParquet2DF, taxiPickupsByDistanceRange, writeDF2Postgres }
import com.dimafeng.testcontainers.{ ForAllTestContainer, PostgreSQLContainer }
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{ Row }
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers
import java.util.Properties
import java.sql.DriverManager


class DistributionByDistanceInfoTest extends SharedSparkSession with ForAllTestContainer with Matchers {

  override val container: PostgreSQLContainer = PostgreSQLContainer()
  val parquetPath = "src/main/resources/data/yellow_taxi_jan_25_2018"

  def getCntrConnProps(): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", container.username)
    connectionProperties.put("password", container.password)
    connectionProperties.put("driver", "org.postgresql.Driver")
    connectionProperties.put("url", container.jdbcUrl)

    connectionProperties
  }


  test("DistributionByDistance mart test.") {
    val taxiDF = loadParquet2DF( parquetPath )
    val distanceDistribution = taxiPickupsByDistanceRange( taxiDF )

    checkAnswer( distanceDistribution.limit( 2 ),
      Row( 0.0, 15088, 0.01, 0.38, 2.88, 5.8, 0.72, 2.01, 0.25)  ::
      Row( 0.1, 35616, 0.48, 0.6,  4.73, 6.96, 1.0, 1.47, 0.21) ::  Nil
    )
  }


  test("DistributionByDistance write to Postgres test.") {
    val properties = getCntrConnProps()

    val taxiDF = loadParquet2DF( parquetPath )
    val distanceDistribution = taxiPickupsByDistanceRange( taxiDF )

    writeDF2Postgres( distanceDistribution, "testTbl", properties)

    val conn = DriverManager.getConnection( container.jdbcUrl, properties)
    try {
      val stmt = conn.prepareStatement("select count(*) cnt from testTbl")
      val rs = stmt.executeQuery()
      rs.next()
      val cnt = rs.getInt( 1 )
      println( s">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> cnt = $cnt")

      cnt should equal( 11 )
    }
    finally {
      conn.close()
    }
  }

}
