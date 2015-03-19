/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

class SubscriberProfilingViewTest extends FlatSpec with ShouldMatchers {

  import SubscriberProfilingView._

  trait WithSubscriberProfilingView {

    val line = "420030100040377|16-25|M|Saudi Arabia|Top 20%"
    val wrongLine = "420030100040377|16-25"
    val fields: Array[String] = Array("420030100040377", "16-25", "M", "Saudi Arabia", "Top 20%")
    val header: Array[String] = Array("imsi", "ageGroup", "genderGroup", "nationalityGroup", "affluenceGroup")

    val row = Row("420030100040377", "16-25", "M", "Saudi Arabia", "Top 20%")
    val wrongRow = Row("420030100040377", "16-25", 1, "Saudi Arabia", "Top 20%")

    val subscriberProfilingView =
      SubscriberProfilingView(
        imsi = "420030100040377",
        ageGroup = "16-25",
        genderGroup = "M",
        nationalityGroup = "Saudi Arabia",
        affluenceGroup = "Top 20%")
  }

  trait WithSubscriber {

    val subscriber = Subscriber(
      user = User(
        imei = "3581870526733101",
        imsi = "420030100040377",
        msisdn = 966544312356L),
      idType =  Visa,
      idNumber = Some(1016603803l),
      age = Some(39.75f),
      gender = Male,
      siteId = Some(5049),
      regionId = Some(3),
      nationalities = Nationalities("SAUDI ARABIA", "SAUDI ARABIA"),
      types = SubscriberTypes(PrePaid, "SamsungI930000"),
      packages = SubscriberPackages(Voice, RetailCustomer),
      date = SubscriberDates(Some(1367355600000l), Some(1406840400000l), Some(1406840400000l)),
      activeStatus  = Active,
      sourceActivation = Siebel,
      roamingStatus = "Saudi Arabia",
      currentBalance = Some(100.050000f),
      calculatedSegment = S50,
      revenues = Revenues(
        m1 = 99.04f,
        m2 = 68.57f,
        m3 = 133.77f,
        m4 = 109.99f,
        m5 = 106.36f,
        m6 = 125.23f))
  }

  trait WithAgeGroup extends WithSubscriber {

    val ageGroup16To25 = "16-25"
    val ageGroup26To60 = "26-60"
    val ageGroupGreaterThan60 = "61-"
  }

  trait WithNationalities extends WithSubscriber {

    val nationalitiesSaudiArabia = Nationalities(declared = "", inferred = "Saudi Arabia")
    val nationalitiesSaudiArabiaUpperCase =
      nationalitiesSaudiArabia.copy(inferred = nationalitiesSaudiArabia.inferred.toUpperCase)
    val nationalitiesSaudiArabiaLowerCase =
      nationalitiesSaudiArabia.copy(inferred = nationalitiesSaudiArabia.inferred.toLowerCase)

    val nationalitiesOther = Nationalities(declared = "", inferred = "Other")

    val imsiSaudiArabia = "420030100040377"
    val imsiOther = "412010100040377"

    val nationalityGroupSaudiArabia = "Saudi Arabia"
    val nationalityGroupRoamers = "Roamers"
    val nationalityGroupNonSaudi = "Non-Saudi"
  }

  trait WithAffluenceGroup extends WithSubscriber {

    val max = 100

    val affluenceGroupTop20 = "Top 20%"
    val affluenceGroupMiddle30 = "Middle 30%"
    val affluenceGroupBottom = "Bottom 50%"
  }

  trait WithTotalRevenue extends WithSubscriber {

    val subscriberLargeCorporate = subscriber.copy(packages = subscriber.packages.copy(corp = LargeCorporate))

    val subscriberPostPaid = subscriber.copy(types = subscriber.types.copy(pay = PostPaid))

    val maxTotalRevenue = MaxTotalRevenue
    val maxTotalRevenueValue = 99999999
  }

  "SubscriberProfilingView" should "return correct fields" in new WithSubscriberProfilingView {
    subscriberProfilingView.fields should be (fields)
  }

  it should "return correct header" in new WithSubscriberProfilingView {
    SubscriberProfilingView.Header should be (header)
  }

  it should "field and header have same size" in new WithSubscriberProfilingView {
    subscriberProfilingView.fields.size should be (SubscriberProfilingView.Header.size)
  }

  it should "be built from CSV" in new WithSubscriberProfilingView {
    CsvParser.fromLine(line).value.get should be (subscriberProfilingView)
  }

  it should "be discarded when CSV format is wrong" in new WithSubscriberProfilingView {
    an [Exception] should be thrownBy CsvParser.fromLine(wrongLine).value.get
  }
  
  it should "be built from Row" in new WithSubscriberProfilingView {
    fromRow.fromRow(row) should be (subscriberProfilingView)
  }

  it should "be discarded when row is wrong" in new WithSubscriberProfilingView {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "return 16-25 as age group when age between 16 and 25" in new WithAgeGroup {
    for (i <- 16 to 25)
      SubscriberProfilingView.ageGroup(subscriber.copy(age = Some(i.toFloat))) should be (ageGroup16To25)
  }

  it should "return 26-60 as age group when age between 26 and 60" in new WithAgeGroup {
    for (i <- 26 to 60)
      SubscriberProfilingView.ageGroup(subscriber.copy(age = Some(i.toFloat))) should be (ageGroup26To60)
  }

  it should "return 61- as age group when age greater than 60" in new WithAgeGroup {
    for (i <- 61 to 110)
      SubscriberProfilingView.ageGroup(subscriber.copy(age = Some(i.toFloat))) should be (ageGroupGreaterThan60)
  }

  it should "return Unknown as age group when age not defined" in new WithSubscriber {
    SubscriberProfilingView.ageGroup(subscriber.copy(age = None)) should be (EdmCoreUtils.UnknownKeyword)
  }

  it should "return Unknown as age group when age less than 16" in new WithSubscriber {
    for (i <- 0 to 15)
      SubscriberProfilingView.ageGroup(subscriber.copy(age = Some(i.toFloat))) should be (EdmCoreUtils.UnknownKeyword)
  }

  it should "return correct male gender group" in new WithSubscriber {
    SubscriberProfilingView.genderGroup(subscriber.copy(gender = Male)) should be ("M")
  }

  it should "return correct female gender group" in new WithSubscriber {
    SubscriberProfilingView.genderGroup(subscriber.copy(gender = Female)) should be ("F")
  }

  it should "return unkwown gender group" in new WithSubscriber {
    SubscriberProfilingView.genderGroup(subscriber.copy(gender = UnknownGender)) should be (EdmCoreUtils.UnknownKeyword)
  }

  it should "return Saudi Arabia when nationality inferred is Saudi Arabia" in new WithNationalities {
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabia,
        user = subscriber.user.copy(imsi = imsiSaudiArabia))) should be (nationalityGroupSaudiArabia)
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabia,
        user = subscriber.user.copy(imsi = ""))) should be (nationalityGroupSaudiArabia)
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabia,
        user = subscriber.user.copy(imsi = imsiOther))) should be (nationalityGroupSaudiArabia)

    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabiaUpperCase,
        user = subscriber.user.copy(imsi = imsiSaudiArabia))) should be (nationalityGroupSaudiArabia)
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabiaUpperCase,
        user = subscriber.user.copy(imsi = ""))) should be (nationalityGroupSaudiArabia)
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabiaUpperCase,
        user = subscriber.user.copy(imsi = imsiOther))) should be (nationalityGroupSaudiArabia)

    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabiaLowerCase,
        user = subscriber.user.copy(imsi = imsiSaudiArabia))) should be (nationalityGroupSaudiArabia)
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabiaLowerCase,
        user = subscriber.user.copy(imsi = ""))) should be (nationalityGroupSaudiArabia)
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesSaudiArabiaLowerCase,
        user = subscriber.user.copy(imsi = imsiOther))) should be (nationalityGroupSaudiArabia)
  }

  it should "return roamers when nationality inferred is not Saudi Arabia and MCC is not Saudi Arabia" in new
      WithNationalities {
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesOther,
        user = subscriber.user.copy(imsi = imsiOther))) should be (nationalityGroupRoamers)
  }

  it should "return non saudi when nationality inferred is not Saudi Arabia and MCC is not Saudi Arabia" in new
      WithNationalities {
    SubscriberProfilingView.nationalityGroup(
      subscriber.copy(
        nationalities = nationalitiesOther,
        user = subscriber.user.copy(imsi = imsiSaudiArabia))) should be (nationalityGroupNonSaudi)
  }

  it should "return Top 20% if order between 100 and 81 and max 100" in new WithAffluenceGroup {
    for (i <- 81 to 100)
      SubscriberProfilingView.affluenceGroup(i, max) should be (affluenceGroupTop20)
  }

  it should "return Middle 30% if order between 80 and 51 and max 100" in new WithAffluenceGroup {
    for (i <- 51 to 80)
      SubscriberProfilingView.affluenceGroup(i, max) should be (affluenceGroupMiddle30)
  }

  it should "return Bottom 50% if order between 50 and 0 and max 100" in new WithAffluenceGroup {
    for (i <- 0 to 50)
      SubscriberProfilingView.affluenceGroup(i, max) should be (affluenceGroupBottom)
  }

  it should "return correct maximum total revenue" in new WithTotalRevenue {
    SubscriberProfilingView.MaxTotalRevenue should be (maxTotalRevenueValue)
  }

  it should "return maximum total revenue when total revenue and subscriber large corporate" in new WithTotalRevenue {
    SubscriberProfilingView.totalRevenue(subscriberLargeCorporate) should be (maxTotalRevenue)
  }

  it should "return maximum total revenue when total revenue and subscriber post paid" in new WithTotalRevenue {
    SubscriberProfilingView.totalRevenue(subscriberPostPaid) should be (maxTotalRevenue)
  }

  it should "return total revenue when total revenue and subscriber neither large corporate not post paid" in
    new WithTotalRevenue {
    SubscriberProfilingView.totalRevenue(subscriber) should be (subscriber.revenues.totalRevenue)
  }

  it should "return age groups" in new WithSubscriber {
    SubscriberProfilingView.AgeGroups should be (Array("16-25", "26-60", "61-", EdmCoreUtils.UnknownKeyword))
  }

  it should "return gender groups" in new WithSubscriber {
    SubscriberProfilingView.GenderGroups should be (Array("M", "F", EdmCoreUtils.UnknownKeyword))
  }

  it should "return nationality groups" in new WithSubscriber {
    SubscriberProfilingView.NationalityGroups should be (Array("Saudi Arabia", "Roamers", "Non-Saudi"))
  }

  it should "return affluence groups" in new WithSubscriber {
    SubscriberProfilingView.affluenceGroups should be (Array("Top 20%", "Middle 30%", "Bottom 50%"))
  }
}
