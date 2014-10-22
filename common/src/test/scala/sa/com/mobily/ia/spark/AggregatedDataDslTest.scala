/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class AggregatedDataDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

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

  "AggregatedDsl" should "get correctly parsed aggregated data" in new WithAggregatedDatasText {
    aggregatedDatas.toAggregatedData.count should be (2)
  }

  it should "get errors when parsing Aggregated Data" in new WithAggregatedDatasText {
    aggregatedDatas.toAggregatedDataErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed Aggregated Data" in new WithAggregatedDatasText {
    aggregatedDatas.toParsedAggregatedData.count should be (3)
  }
}
