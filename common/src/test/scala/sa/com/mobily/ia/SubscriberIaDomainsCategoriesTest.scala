/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaDomainsCategoriesTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaDomainsCategories._

  trait WithSubscriberIaDomainsCategories {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberDomainsCategoriesLine =
      "\"20141001\"|\"87865881\"|\"100120\"|\"933\"|\"663082\"|\"15365627\"|\"16028709\"|\"\"|\"101\""
    val fields = Array("20141001", "87865881", "100120", "933", "663082", "15365627", "16028709", "", "101")

    val subscriberIaDomainsCategoriesFields =
      Array("20141001", "87865881", "100120", "933", "663082.0", "1.5365627E7", "1.6028709E7", "", "101")
    val subscriberIaDomainsCategoriesHeader =
      Array("date", "subscriberId", "categoryId") ++
        TrafficInfo.Header ++
        Array("locationId", "businessEntityId")

    val row = Row(timestamp, "87865881", "100120", Row(933L, 663082D, 15365627D, 16028709D), "", "101")
    val wrongRow = Row("NaN", "87865881", "100120", Row(933L, 663082D, 15365627D, 16028709D), "", "101")
    
    val subscriberIaDomainsCategories =
      SubscriberIaDomainsCategories(
        timestamp = timestamp,
        subscriberId = "87865881",
        categoryId = "100120",
        trafficInfo = TrafficInfo(
          visitCount = 933L,
          uploadVolume = 663082D,
          downloadVolume = 15365627D,
          totalVolume = 16028709D),
        locationId = "",
        businessEntityId = "101")
  }

  "SubscriberIaDomainsCategories" should "return correct header" in new WithSubscriberIaDomainsCategories {
    SubscriberIaDomainsCategories.Header should be (subscriberIaDomainsCategoriesHeader)
  }

  it should "return correct fields" in new WithSubscriberIaDomainsCategories {
    subscriberIaDomainsCategories.fields should be (subscriberIaDomainsCategoriesFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIaDomainsCategories {
    subscriberIaDomainsCategories.fields.length should be (SubscriberIaDomainsCategories.Header.length)
  }
  
  it should "be built from CSV" in new WithSubscriberIaDomainsCategories {
    CsvParser.fromLine(subscriberDomainsCategoriesLine).value.get should be (subscriberIaDomainsCategories)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaDomainsCategories {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaDomainsCategories {
    SubscriberIaDomainsCategories.Fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaDomainsCategories {
    SubscriberIaDomainsCategories.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaDomainsCategories {
    an [IllegalArgumentException] should be thrownBy SubscriberIaDomainsCategories.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from Row" in new WithSubscriberIaDomainsCategories {
    fromRow.fromRow(row) should be (subscriberIaDomainsCategories)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIaDomainsCategories {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
