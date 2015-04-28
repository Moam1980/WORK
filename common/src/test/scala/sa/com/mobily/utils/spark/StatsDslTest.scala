/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.utils.{LocalSparkSqlContext, Stats}

class StatsDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import StatsDsl._
  
  trait WithStatsText {

    val stats1 =
      "\"5\"|\"4.0\"|\"1.0\"|\"2.4\"|\"1.3000000000000003\"|\"1.140175425099138\"|\"4.0\"|\"4.0\"|\"4.0\"|" +
        "\"3.5\"|\"2.0\"|\"1.5\"|\"1.0\"|\"1.0\""
    val stats2 =
      "\"125\"|\"95.6\"|\"12.334\"|\"60.12\"|\"4.56\"|\"20.79\"|\"90.01\"|\"88.78\"|\"84.11\"|" +
        "\"75.67\"|\"66.33\"|\"63.22\"|\"60.12\"|\"50.12\""
    val stats3 =
      "\"NaN\"|\"4.0\"|\"1.0\"|\"2.4\"|\"1.3000000000000003\"|\"1.140175425099138\"|\"4.0\"|\"4.0\"|\"4.0\"|" +
        "\"3.5\"|\"2.0\"|\"1.5\"|\"1.0\"|\"1.0\""

    val stats = sc.parallelize(List(stats1, stats2, stats3))
  }

  trait WithStatsRows {

    val row = Row(5L, 4.0D, 1.0D, 2.4D, 1.3000000000000003D, 1.140175425099138D, 4D, 4D, 4D, 3.5D, 2D, 1.5D, 1D, 1D)
    val row2 =
      Row(125L, 95.6D, 12.334D, 60.12D, 4.56D, 20.79D, 90.01D, 88.78D, 84.11D, 75.67D, 66.33D, 63.22D, 60.12D, 50.12D)
    val wrongRow =
      Row("NaN", 4.0D, 1.0D, 2.4D, 1.3000000000000003D, 1.140175425099138D, 4D, 4D, 4D, 3.5D, 2D, 1.5D, 1D, 1D)

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithStats {
    
    val stats1 =
      Stats(
        count = 5L,
        max = 4D,
        min = 1D,
        mean = 2.4D,
        variance = 1.3000000000000003D,
        stDev = 1.140175425099138D,
        percentile99 = 4D,
        percentile95 = 4D,
        percentile90 = 4D,
        percentile75 = 3.5D,
        percentile50 = 2D,
        percentile25 = 1.5D,
        percentile10 = 1D,
        percentile5 = 1D)
    val stats2 =
      Stats(
        count = 125L,
        max = 95.6D,
        min = 12.334D,
        mean = 60.12D,
        variance = 4.56D,
        stDev = 20.79D,
        percentile99 = 90.01D,
        percentile95 = 88.78D,
        percentile90 = 84.11D,
        percentile75 = 75.67D,
        percentile50 = 66.33D,
        percentile25 = 63.22D,
        percentile10 = 60.12D,
        percentile5 = 50.12D)

    val stats = sc.parallelize(List(stats1, stats2))
  }

  trait WithRddDouble {

    val rddDouble = sc.parallelize(List(1D, 2D, 3D, 4D))

    val stats = Stats(
      count = 4L,
      max = 4D,
      min = 1D,
      mean = 2.5D,
      variance = 1.25D,
      stDev = 1.118033988749895D,
      percentile99 = 4D,
      percentile95 = 4D,
      percentile90 = 4D,
      percentile75 = 3.75D,
      percentile50 = 2.5D,
      percentile25 = 1.25D,
      percentile10 = 1D,
      percentile5 = 1D)

    val rddDoubleOneElement = sc.parallelize(List(1D))

    val statsOneElement = Stats(
      count = 1L,
      max = 1D,
      min = 1D,
      mean = 1D,
      variance = 0D,
      stDev = 0D,
      percentile99 = 1D,
      percentile95 = 1D,
      percentile90 = 1D,
      percentile75 = 1D,
      percentile50 = 1D,
      percentile25 = 1D,
      percentile10 = 1D,
      percentile5 = 1D)

    val rddDoubleNoElements = sc.parallelize(List[Double]())

    val statsNoElements = Stats(
      count = 0L,
      max = 0D,
      min = 0D,
      mean = 0D,
      variance = 0D,
      stDev = 0D,
      percentile99 = 0D,
      percentile95 = 0D,
      percentile90 = 0D,
      percentile75 = 0D,
      percentile50 = 0D,
      percentile25 = 0D,
      percentile10 = 0D,
      percentile5 = 0D)
  }

  "StatsDsl" should "get correctly parsed data" in new WithStatsText {
    stats.toStats.count should be (2)
  }

  it should "get errors when parsing data" in new WithStatsText {
    stats.toStatsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithStatsText {
    stats.toParsedStats.count should be (3)
  }

  it should "get correctly parsed rows" in new WithStatsRows {
    rows.toStats.count should be (2)
  }

  it should "save in parquet" in new WithStats {
    val path = File.makeTemp().name
    stats.saveAsParquetFile(path)
    sqc.parquetFile(path).toStats.collect should be (stats.collect)
    File(path).deleteRecursively
  }

  it should "to stats from RDD of Double" in new WithRddDouble {
    StatsDsl.statsHelper.toStats(rddDouble) should be (stats)
  }

  it should "to stats from RDD of Double only one element" in new WithRddDouble {
    StatsDsl.statsHelper.toStats(rddDoubleOneElement) should be (statsOneElement)
  }

  it should "to stats from RDD of Double no elements" in new WithRddDouble {
    StatsDsl.statsHelper.toStats(rddDoubleNoElements) should be (statsNoElements)
  }
}
