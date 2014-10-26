/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberIaDomainsCategoriesTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaDomainsCategories._

  trait WithSubscriberIaDomainsCategories {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberDomainsCategoriesLine = "\"20141001\"|\"87865881\"|\"100120\"|\"933\"|\"663082\"|\"15365627\"|" +
      "\"16028709\"|\"\"|\"101\""
    val fields = Array("20141001", "87865881", "100120", "933", "663082", "15365627", "16028709", "", "101")

    val subscriberDomainsCategories = SubscriberIaDomainsCategories(timestamp, "87865881", "100120", 933L, 663082D,
      15365627D, 16028709D, "", "101")
  }

  "SubscriberIaDomainsCategories" should "be built from CSV" in new WithSubscriberIaDomainsCategories {
    CsvParser.fromLine(subscriberDomainsCategoriesLine).value.get should be (subscriberDomainsCategories)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaDomainsCategories {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaDomainsCategories {
    SubscriberIaDomainsCategories.fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaDomainsCategories {
    SubscriberIaDomainsCategories.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaDomainsCategories {
    an [IllegalArgumentException] should be thrownBy SubscriberIaDomainsCategories.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
