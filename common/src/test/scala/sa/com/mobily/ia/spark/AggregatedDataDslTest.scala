/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.AggregatedData
import sa.com.mobily.utils.LocalSparkSqlContext

class AggregatedDataDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import AggregatedDataDsl._

  trait WithAggregatedDatasText {

    val aggregatedData1 = "\"20141001\"|\"1446145\"|\"0\"|\"388620093\"|\"388657969\"|\"2148741728993\"" +
      "|\"55248290629619\"|\"57397032358612\"|\"37876\"|\"0\"|\"101\""
    val aggregatedData2 = "\"20141002\"|\"9999999\"|\"20202\"|\"123456789\"|\"987654321\"|\"123456789012\"" +
      "|\"98765432109876\"|\"678901234567890\"|\"55555\"|\"3333333\"|\"UNIT\""
    val aggregatedData3 = "\"20141001\"|\"1446145\"|\"0\"|\"388620093\"|\"388657969\"|\"2148741728993\"" +
      "|\"55248290629619\"|\"57397032358612\"|\"text\"|\"0\"|\"101\""

    val aggregatedDatas = sc.parallelize(List(aggregatedData1, aggregatedData2, aggregatedData3))
  }

  trait WithAggregatedDataRows {

    val row =
      Row(1412110800000L, 1446145L, 0L, 388620093L, 388657969L, 2148741728993D, 55248290629619D, 57397032358612D,
        37876L, 0L, "101")
    val row2 =
      Row(1412208000000L, 9999999L, 20202L, 123456789L, 987654321L, 123456789012D, 98765432109876D, 678901234567890D,
        55555L, 3333333L, "UNIT")
    val wrongRow =
      Row(1412110800000L, 1446145L, 0L, 388620093L, 388657969L, 2148741728993D, 55248290629619D, 57397032358612D,
        "text", 0L, "101")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithAggregatedData {

    val aggregatedData1 =
      AggregatedData(
        timestamp = 1412110800000L,
        subscriberCount = 1446145L,
        urlValidCount = 0L,
        urlCrawlCount = 388620093L,
        urlIndexCount = 388657969L,
        uploadVolume = 2148741728993D,
        downloadVolume = 55248290629619D,
        totalVolume = 57397032358612D,
        keywordSearchCount = 37876L,
        keywordContentCount = 0L,
        businessEntityId = "101")
    val aggregatedData2 =
      AggregatedData(
        timestamp = 1412208000000L,
        subscriberCount = 9999999L,
        urlValidCount = 20202L,
        urlCrawlCount = 123456789L,
        urlIndexCount = 987654321L,
        uploadVolume = 123456789012D,
        downloadVolume = 98765432109876D,
        totalVolume = 678901234567890D,
        keywordSearchCount = 55555L,
        keywordContentCount = 3333333L,
        businessEntityId = "UNIT")
    val aggregatedData3 =
      AggregatedData(
        timestamp = 1412110800000L,
        subscriberCount = 1446145L,
        urlValidCount = 0L,
        urlCrawlCount = 388620093L,
        urlIndexCount = 388657969L,
        uploadVolume = 2148741728993D,
        downloadVolume = 55248290629619D,
        totalVolume = 57397032358612D,
        keywordSearchCount = 37876L,
        keywordContentCount = 0L,
        businessEntityId = "101")

    val aggregatedData = sc.parallelize(List(aggregatedData1, aggregatedData2, aggregatedData3))
  }

  "AggregatedDsl" should "get correctly parsed aggregated data" in new WithAggregatedDatasText {
    aggregatedDatas.toAggregatedData.count should be (2)
  }

  it should "get errors when parsing Aggregated Data" in new WithAggregatedDatasText {
    aggregatedDatas.toAggregatedDataErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed Aggregated Data" in new WithAggregatedDatasText {
    aggregatedDatas.toParsedAggregatedData.count should be (3)
  }

  it should "get correctly parsed rows" in new WithAggregatedDataRows {
    rows.toAggregatedData.count should be (2)
  }

  it should "save in parquet" in new WithAggregatedData {
    val path = File.makeTemp().name
    aggregatedData.saveAsParquetFile(path)
    sqc.parquetFile(path).toAggregatedData.collect should be (aggregatedData.collect)
    File(path).deleteRecursively
  }
}
