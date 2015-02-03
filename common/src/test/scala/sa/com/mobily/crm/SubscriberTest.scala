/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User

class SubscriberTest extends FlatSpec with ShouldMatchers {

  import Subscriber._

  trait WithCustomerSubscriber {

    val line = "966544312356|Visa|1016603803|3581870526733101|420030100040377|M|5049|3|Saudi Arabia|KSA" +
      "|Pre-Paid|Voice|39.75|Retail Customer|5/1/2013|Active|Siebel|Saudi Arabia|SamsungI930000|100.050000|8/1/2014" +
      "|8/1/2014|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val fields: Array[String] = Array("966544312356","Visa","1016603803","3581870526733101","420030100040377","M",
      "5049","3","Saudi Arabia","KSA","Pre-Paid","Voice","39.75","Retail Customer","5/1/2013","Active","Siebel",
      "Saudi Arabia","SamsungI930000","100.050000","8/1/2014","8/1/2014","S50","99.04","68.57","133.77","109.99",
      "106.36","125.23")

    val subscriberTypes = SubscriberTypes(PrePaid, "SamsungI930000")
    val subscriberPackages = SubscriberPackages(Voice, RetailCustomer)
    val customerSubscriber = Subscriber(
      user = User(
        imei = "3581870526733101",
        imsi = "420030100040377",
        msisdn = 966544312356L),
      idType =  Visa,
      idNumber = Some(1016603803l),
      age = Some(39.75f),
      gender = "M",
      siteId = Some(5049),
      regionId = Some(3),
      nationalies = Nationalities("SAUDI ARABIA", "KSA"),
      types = subscriberTypes,
      packages = subscriberPackages,
      date = SubscriberDates(Some(1367355600000l), Some(1406840400000l), Some(1406840400000l)),
      activeStatus  = Active,
      sourceActivation = Siebel,
      roamingStatus = "Saudi Arabia",
      currentBalance = Some(100.050000f),
      m1CalculatedSegment = S50,
      revenues = Revenues(
        m1 = 99.04f,
        m2 = 68.57f,
        m3 = 133.77f,
        m4 = 109.99f,
        m5 = 106.36f,
        m6 = 125.23f)
    )
  }

  "CustomerSubscriber" should "be built from CSV" in new WithCustomerSubscriber {
    CsvParser.fromLine(line).value.get should be (customerSubscriber)
  }

  it should "be discarded when CSV format is wrong" in new WithCustomerSubscriber {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(0, "0").updated(3, "").updated(4,""))
  }

  it should "be built from CSV with an Unkwonw identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, "A")) should be (customerSubscriber.copy(idType = UnknownIdType))
  }

  it should "be built from CSV with Passport identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, Passport.id)) should be (customerSubscriber.copy(idType = Passport))
  }

  it should "be built from CSV with Ncbs identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, Ncbs.id)) should be (customerSubscriber.copy(idType = Ncbs))
  }

  it should "be built from CSV with Invalid identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, Invalid.id)) should be (customerSubscriber.copy(idType = Invalid))
  }

  it should "be built from CSV with Saudi National identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, SaudiNational.id)) should
      be (customerSubscriber.copy(idType = SaudiNational))
  }

  it should "be built from CSV with Gcc identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, Gcc.id)) should be (customerSubscriber.copy(idType = Gcc))
  }

  it should "be built from CSV with Diplomatic Card identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, DiplomaticCard.id)) should
      be (customerSubscriber.copy(idType = DiplomaticCard))
  }

  it should "be built from CSV with Tax Number identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, TaxNumber.id)) should be (customerSubscriber.copy(idType = TaxNumber))
  }

  it should "be built from CSV with Mobily Employee identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, MobilyEmployee.id)) should
      be (customerSubscriber.copy(idType = MobilyEmployee))
  }

  it should "be built from CSV with Commercial Registration identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, CommercialRegistration.id)) should
      be (customerSubscriber.copy(idType = CommercialRegistration))
  }

  it should "be built from CSV with Mobily Cost Center identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, MobilyCostCenter.id)) should
      be (customerSubscriber.copy(idType = MobilyCostCenter))
  }

  it should "be built from CSV with Iqama identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, Iqama.id)) should
      be (customerSubscriber.copy(idType = Iqama))
  }

  it should "be built from CSV with Family Card identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, FamilyCard.id)) should
      be (customerSubscriber.copy(idType = FamilyCard))
  }

  it should "be built from CSV with Ms identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, Ms.id)) should be (customerSubscriber.copy(idType = Ms))
  }

  it should "be built from CSV with Driver Licence Number identification type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(1, DriverLicenceNumber.id)) should
      be (customerSubscriber.copy(idType = DriverLicenceNumber))
  }

  it should "be built from CSV with Post-Paid pay type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(10, PostPaid.id)).types should be (subscriberTypes.copy(pay = PostPaid))
  }

  it should "be built from CSV with Unknown pay type" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(10, "A")).types should be (subscriberTypes.copy(pay = UnknownPayType))
  }

  it should "be built from CSV with Ots active status" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(15, Ots.id)) should be (customerSubscriber.copy(activeStatus = Ots))
  }

  it should "be built from CSV with HotSIM active status" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(15, HotSIM.id)) should be (customerSubscriber.copy(activeStatus = HotSIM))
  }

  it should "be built from CSV with Unknown active status" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(15, "A")) should be (customerSubscriber.copy(activeStatus = UnknownActiveStatus))
  }

  it should "be built from CSV with Data datapackage" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(11, Data.id)).packages should be (subscriberPackages.copy(data = Data))
  }

  it should "be built from CSV with Unknown datapackage" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(11, "A")).packages should be (subscriberPackages.copy(data = UnknownDataPackage))
  }

  it should "be built from CSV with Iuc corp package" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(13, Iuc.id)).packages should be (subscriberPackages.copy(corp = Iuc))
  }

  it should "be built from CSV with Large Corporate corp package" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(13, LargeCorporate.id)).packages should
      be (subscriberPackages.copy(corp = LargeCorporate))
  }

  it should "be built from CSV with Unknown corp package" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(13, "A")).packages should be (subscriberPackages.copy(corp = UnknownCorpPackage))
  }

  it should "be built from CSV with Mcr source activation" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(16, Mcr.id)) should be (customerSubscriber.copy(sourceActivation = Mcr))
  }

  it should "be built from CSV with Mdm source activation" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(16, Mdm.id)) should be (customerSubscriber.copy(sourceActivation = Mdm))
  }

  it should "be built from CSV with Unknown source activation" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(16, "A")) should
      be (customerSubscriber.copy(sourceActivation = UnknownSourceActivation))
  }

  it should "be built from CSV with W segmentc calculated" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(22, Wcs.id)) should be (customerSubscriber.copy(m1CalculatedSegment = Wcs))
  }

  it should "be built from CSV with S40 segmentc calculated" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(22, S40.id)) should be (customerSubscriber.copy(m1CalculatedSegment = S40))
  }

  it should "be built from CSV with S60 segmentc calculated" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(22, S60.id)) should be (customerSubscriber.copy(m1CalculatedSegment = S60))
  }

  it should "be built from CSV with S80 segmentc calculated" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(22, S80.id)) should be (customerSubscriber.copy(m1CalculatedSegment = S80))
  }

  it should "be built from CSV with S90 segmentc calculated" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(22, S90.id)) should be (customerSubscriber.copy(m1CalculatedSegment = S90))
  }

  it should "be built from CSV with Unknown segmentc calculated" in new WithCustomerSubscriber {
    fromCsv.fromFields(fields.updated(22, "A")) should
      be (customerSubscriber.copy(m1CalculatedSegment = UnknownSourceCalculatedSegment))
  }
}
