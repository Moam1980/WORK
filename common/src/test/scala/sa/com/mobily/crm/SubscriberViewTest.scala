/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.utils.EdmCoreUtils

class SubscriberViewTest extends FlatSpec with ShouldMatchers {

  import SubscriberView._

  trait WithSubscriberView {

    val line = "420030100040377|39.75|M|SAUDI ARABIA|KSA|Pre-Paid|SamsungI930000|Voice|Retail " +
      "Customer|1367355600000|1406840400000|1406840400000|Active|100.05|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val fields: Array[String] = Array("420030100040377", "39.75", "M", "SAUDI ARABIA", "KSA", "Pre-Paid",
      "SamsungI930000", "Voice", "Retail Customer", "1367355600000", "1406840400000", "1406840400000", "Active",
      "100.05", "S50", "99.04", "68.57", "133.77", "109.99", "106.36", "125.23")
    val header: Array[String] = Array("imsi", "age", "gender", "nationality-declared", "nationality-inferred",
      "paytype", "handsetType", "packages-data", "packages-corp", "activation-date", "last-activity-date",
      "last-recharge-date", "active-status", "currentBalance", "calculated-segment", "revenues-m1", "revenues-m2",
      "revenues-m3", "revenues-m4", "revenues-m5", "revenues-m6")

    val subscriberTypes = SubscriberTypes(PrePaid, "SamsungI930000")
    val subscriberPackages = SubscriberPackages(Voice, RetailCustomer)
    val subscriberView = SubscriberView(
      imsi = "420030100040377",
      age = Some(39.75f),
      gender = "M",
      nationalities = Nationalities("SAUDI ARABIA", "KSA"),
      types = subscriberTypes,
      packages = subscriberPackages,
      date = SubscriberDates(Some(1367355600000l), Some(1406840400000l), Some(1406840400000l)),
      activeStatus  = Active,
      currentBalance = Some(100.050000f),
      calculatedSegment = S50,
      revenues = Revenues(
        m1 = 99.04f,
        m2 = 68.57f,
        m3 = 133.77f,
        m4 = 109.99f,
        m5 = 106.36f,
        m6 = 125.23f)
    )
    val resultCsv = SubscriberView.header ++ fields.mkString(EdmCoreUtils.Separator)
  }

  "SubscriberView" should "be built from CSV" in new WithSubscriberView {
    CsvParser.fromLine(line).value.get should be (subscriberView)
  }

  it should "be discarded when CSV format is wrong" in new WithSubscriberView {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(15, ""))
  }

  it should "return correct fields" in new WithSubscriberView {
    subscriberView.fields should be (fields)
  }

  it should "return correct header" in new WithSubscriberView {
    SubscriberView.header should be (header)
  }

  it should "field and header have same size" in new WithSubscriberView {
    subscriberView.fields.size should be (SubscriberView.header.size)
  }
}
