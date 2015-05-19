/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class StatsTest extends FlatSpec with ShouldMatchers {

  import Stats._

  trait WithStats {

    val array = Array(1D, 3D, 4D, 2D, 2D)

    val statsLine =
      "\"5\"|\"4.0\"|\"1.0\"|\"2.4\"|\"1.0400000000000003\"|\"1.019803902718557\"|\"4.0\"|\"4.0\"|\"4.0\"|" +
        "\"3.5\"|\"2.0\"|\"1.5\"|\"1.0\"|\"1.0\""
    val fields =
      Array("5", "4.0", "1.0", "2.4", "1.0400000000000003", "1.019803902718557", "4.0", "4.0", "4.0", "3.5", "2.0",
        "1.5", "1.0", "1.0")

    val statsFields =
      Array("5", "4.0", "1.0", "2.4", "1.0400000000000003", "1.019803902718557", "4.0", "4.0", "4.0", "3.5", "2.0",
        "1.5", "1.0", "1.0")
    val statsHeader =
      Array("count", "max", "min", "mean", "variance", "stDev", "percentile99", "percentile95", "percentile90",
        "percentile75", "percentile50", "percentile25", "percentile10", "percentile5")

    val row = Row(5L, 4.0D, 1.0D, 2.4D, 1.0400000000000003D, 1.019803902718557D, 4D, 4D, 4D, 3.5D, 2D, 1.5D, 1D, 1D)
    val wrongRow =
      Row("NaN", 4.0D, 1.0D, 2.4D, 1.0400000000000003D, 1.019803902718557D, 4D, 4D, 4D, 3.5D, 2D, 1.5D, 1D, 1D)

    val stats =
      Stats(
        count = 5L,
        max = 4D,
        min = 1D,
        mean = 2.4D,
        variance = 1.0400000000000003D,
        stDev = 1.019803902718557D,
        percentile99 = 4D,
        percentile95 = 4D,
        percentile90 = 4D,
        percentile75 = 3.5D,
        percentile50 = 2D,
        percentile25 = 1.5D,
        percentile10 = 1D,
        percentile5 = 1D)
  }

  trait WithArrayStats {

    val emptyArrayDouble = Array[Double]()
    val emptyArrayStats = Array[Stats]()
    val emptyStats =
      Stats(
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

    val array1 = Array(1D, 3D, 4D, 2D, 2D)
    val stats1 =
      Stats(
        count = 5L,
        max = 4D,
        min = 1D,
        mean = 2.4D,
        variance = 1.0400000000000003D,
        stDev = 1.019803902718557D,
        percentile99 = 4D,
        percentile95 = 4D,
        percentile90 = 4D,
        percentile75 = 3.5D,
        percentile50 = 2D,
        percentile25 = 1.5D,
        percentile10 = 1D,
        percentile5 = 1D)

    val array2 = Array(100D, 900D, 300D, 1050D, 1200D, 1100D)
    val stats2 =
      Stats(
        count = 6L,
        max = 1200D,
        min = 100D,
        mean = 775D,
        variance = 176458.33333333334D,
        stDev = 420.06943870428535D,
        percentile99 = 1200D,
        percentile95 = 1200D,
        percentile90 = 1200D,
        percentile75 = 1125D,
        percentile50 = 975D,
        percentile25 = 250D,
        percentile10 = 100D,
        percentile5 = 100D)

    val arrayMean = Array(2.4D, 775D)
    val arrayStats = Array(stats1, stats2)
    val statsStats =
      Stats(
        count = 2L,
        max = 775D,
        min = 2.4D,
        mean = 388.7D,
        variance = 149227.69D,
        stDev = 386.3D,
        percentile99 = 775D,
        percentile95 = 775D,
        percentile90 = 775D,
        percentile75 = 775D,
        percentile50 = 388.7D,
        percentile25 = 2.4D,
        percentile10 = 2.4D,
        percentile5 = 2.4D)
  }

  "Stats" should "return correct header" in new WithStats {
    Stats.Header should be (statsHeader)
  }

  it should "return correct fields" in new WithStats {
    stats.fields should be (statsFields)
  }

  it should "have same number of elements fields and header" in new WithStats {
    stats.fields.length should be (Stats.Header.length)
  }

  it should "be built from CSV" in new WithStats {
    CsvParser.fromLine(statsLine).value.get should be (stats)
  }

  it should "be discarded when the CSV format is wrong" in new WithStats {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "be built from Row" in new WithStats {
    fromRow.fromRow(row) should be (stats)
  }

  it should "be discarded when row is wrong" in new WithStats {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "apply from array double" in new WithArrayStats {
    Stats(array1) should be (stats1)
    Stats(array2) should be (stats2)
  }

  it should "apply from empty array double" in new WithArrayStats {
    Stats(emptyArrayDouble) should be (emptyStats)
  }

  it should "apply from empty array stats" in new WithArrayStats {
    Stats.aggByMean(emptyArrayStats) should be (emptyStats)
  }

  it should "apply from array stats" in new WithArrayStats {
    Stats.aggByMean(arrayStats) should be (statsStats)
  }
}
