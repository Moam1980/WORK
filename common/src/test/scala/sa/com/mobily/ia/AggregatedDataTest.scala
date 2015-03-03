/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class AggregatedDataTest extends FlatSpec with ShouldMatchers {

  import AggregatedData._

  trait WithAggregatedData {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val aggregatedDataLine =
      "\"20141001\"|\"1446145\"|\"0\"|\"388620093\"|\"388657969\"|\"2148741728993\"|\"55248290629619\"" +
        "|\"57397032358612\"|\"37876\"|\"0\"|\"101\""
    val fields =
      Array("20141001", "1446145", "0", "388620093", "388657969", "2148741728993", "55248290629619", "57397032358612",
        "37876", "0", "101")

    val aggregatedDataFields =
      Array("20141001", "1446145", "0", "388620093", "388657969", "2.148741728993E12", "5.5248290629619E13",
        "5.7397032358612E13", "37876", "0", "101")
    val aggregatedDataHeader =
      Array("date", "subscriberCount", "urlValidCount", "urlCrawlCount", "urlIndexCount", "uploadVolume",
        "downloadVolume", "totalVolume", "keywordSearchCount", "keywordContentCount", "businessEntityId")

    val row =
      Row(timestamp, 1446145L, 0L, 388620093L, 388657969L, 2148741728993D, 55248290629619D, 57397032358612D, 37876L,
        0L, "101")
    val wrongRow =
      Row(timestamp, "NAN", 0L, 388620093L, 388657969L, 2148741728993D, 55248290629619D, 57397032358612D, 37876L, 0L,
        "101")
    
    val aggregatedData =
      AggregatedData(
        timestamp = timestamp,
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
  }

  "AggregatedData" should "return correct header" in new WithAggregatedData {
    AggregatedData.Header should be (aggregatedDataHeader)
  }

  it should "return correct fields" in new WithAggregatedData {
    aggregatedData.fields should be (aggregatedDataFields)
  }

  it should "have same number of elements fields and header" in new WithAggregatedData {
    aggregatedData.fields.length should be (AggregatedData.Header.length)
  }
  
  it should "be built from CSV" in new WithAggregatedData {
    CsvParser.fromLine(aggregatedDataLine).value.get should be (aggregatedData)
  }

  it should "be discarded when the CSV format is wrong" in new WithAggregatedData {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithAggregatedData {
    AggregatedData.Fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithAggregatedData {
    AggregatedData.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithAggregatedData {
    an [IllegalArgumentException] should be thrownBy AggregatedData.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from Row" in new WithAggregatedData {
    fromRow.fromRow(row) should be (aggregatedData)
  }

  it should "be discarded when row is wrong" in new WithAggregatedData {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
