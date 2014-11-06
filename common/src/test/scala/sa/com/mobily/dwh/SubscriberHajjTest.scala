/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberHajjTest extends FlatSpec with ShouldMatchers {

  import SubscriberHajj._

  trait WithSubscriberHajj {

    val subscriberDwhLine = "200540851363\tSamsungN710000\tSmartphone\tSamsung\tYes\tSaudi Arabia\tConsumer\tPrepaid" +
      "\tConnect 5G Pre\tConnect\tN\tINTERNAL_VISITOR\tINTERNAL_VISITOR\tLocal\t$null$\t$null$\tLOCALS\t1.000" +
      "\t100.000\tY\t100.000"

    val subscriberDwhLineOther = "966831020866881\t$null$\t$null$\t$null$\t$null$\tSaudi Arabia\tignore\tignore" +
      "\tignore\tignore\tN\tLOCAL\tLOCAL_OLD\tLocal\t$null$\t$null$\tLOCALS\t$null$\t$null$\tN\t$null$"

    val fields = Array("200540851363", "SamsungN710000", "Smartphone", "Samsung", "Yes", "Saudi Arabia", "Consumer",
      "Prepaid", "Connect 5G Pre", "Connect", "N", "INTERNAL_VISITOR", "INTERNAL_VISITOR", "Local", "$null$",
      "$null$", "LOCALS", "1.000", "100.000", "Y", "100.000")

    val subscriberDwh = SubscriberHajj(
      msisdn = 200540851363L,
      handset = Handset(
        handsetType = "SamsungN710000",
        handsetSubCategory = "Smartphone",
        handsetVendor = "Samsung",
        smartPhone = Some(true)),
      hierarchy = SubscriberHierarchy(
        finalFlag = InternalVisitorFlag,
        segment = ConsumerSegment,
        contractType = PrepaidContract,
        category = "Connect",
        pack = "Connect 5G Pre",
        nationalityGroup = National,
        nationality = "Saudi Arabia"),
      makkah7Days = false,
      flagDetails = "INTERNAL_VISITOR",
      totalMou = None,
      destinationCountry = "",
      calculatedNationalityGroup = National,
      numberRecharges = Some(1),
      rechargeAmount = Some(100F),
      subsciberRen = true,
      subsciberRenRev = Some(100F))
  }

  "SubscriberHajjDWH" should "be built from CSV" in new WithSubscriberHajj {
    CsvParser.fromLine(subscriberDwhLine).value.get should be (subscriberDwh)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberHajj {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(0, "WrongNumber"))
  }

  it should "be built from CSV with Business segment" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(6, "Business")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(segment = BusinessSegment)))
  }

  it should "be built from CSV with Ignore segment" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(6, "ignore")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(segment = IgnoreSegment)))
  }

  it should "be built from CSV with Test segment" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(6, "Test")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(segment = TestSegment)))
  }

  it should "be built from CSV with contract type postpaid" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(7, "Postpaid")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(contractType = PostpaidContract)))
  }

  it should "be built from CSV with contract type business" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(7, "Business")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(contractType = BusinessContract)))
  }

  it should "be built from CSV with contract type BB" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(7, "BB")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(contractType = BBContract)))
  }

  it should "be built from CSV with contract type ignore" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(7, "ignore")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(contractType = IgnoreContract)))
  }

  it should "be built from CSV with contract type test" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(7, "test")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(contractType = TestContract)))
  }

  it should "be built from CSV with flag internal visitor" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(11, "Internal_Visitor")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(finalFlag = InternalVisitorFlag)))
  }

  it should "be built from CSV with flag internal" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(11, "Internal")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(finalFlag = InternalFlag)))
  }

  it should "be built from CSV with flag external" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(11, "External")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(finalFlag = ExternalFlag)))
  }

  it should "be built from CSV with flag local" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(11, "Local")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(finalFlag = LocalFlag)))
  }

  it should "be built from CSV with flag default" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(11, "default")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(finalFlag = DefaultFlag)))
  }

  it should "be built from CSV with flag ignore" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(11, "ignore")) should be (subscriberDwh.copy(hierarchy =
      subscriberDwh.hierarchy.copy(finalFlag = IgnoreFlag)))
  }

  it should "be built from CSV with nationality group Expat" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(13, "Expat")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(nationalityGroup = Expat)))
  }

  it should "be built from CSV with nationality group not available" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(13, "N/A")) should be
      (subscriberDwh.copy(hierarchy = subscriberDwh.hierarchy.copy(nationalityGroup = NotAvailable)))
  }

  it should "be built from CSV with calculated nationality group Expat" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(16, "Expat")) should be
      (subscriberDwh.copy(calculatedNationalityGroup = Expat))
  }

  it should "be built from CSV with calculated nationality group not available" in new WithSubscriberHajj {
    fromCsv.fromFields(fields.updated(16, "N/A")) should be
      (subscriberDwh.copy(calculatedNationalityGroup = NotAvailable))
  }
}
