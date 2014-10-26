/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class AggregatedDataTest extends FlatSpec with ShouldMatchers {

  import AggregatedData._

  trait WithAggregatedData {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val aggregatedDataLine = "\"20141001\"|\"1446145\"|\"0\"|\"388620093\"|\"388657969\"|\"2148741728993\"" +
      "|\"55248290629619\"|\"57397032358612\"|\"37876\"|\"0\"|\"101\""
    val fields = Array("20141001", "1446145", "0", "388620093", "388657969", "2148741728993", "55248290629619",
      "57397032358612", "37876", "0", "101")

    val aggregatedData = AggregatedData(timestamp, 1446145L, 0L, 388620093L, 388657969L, 2148741728993D,
      55248290629619D, 57397032358612D, 37876L, 0L, "101")
  }

  "AggregatedData" should "be built from CSV" in new WithAggregatedData {
    CsvParser.fromLine(aggregatedDataLine).value.get should be (aggregatedData)
  }

  it should "be discarded when the CSV format is wrong" in new WithAggregatedData {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithAggregatedData {
    AggregatedData.fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithAggregatedData {
    AggregatedData.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithAggregatedData {
    an [IllegalArgumentException] should be thrownBy AggregatedData.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
