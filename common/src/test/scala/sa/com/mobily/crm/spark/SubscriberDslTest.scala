/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.reflect.io.File

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.crm._
import sa.com.mobily.utils.LocalSparkSqlContext
import sa.com.mobily.user.User

class SubscriberDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberDsl._
  
  trait WithSubscriberText {

    val subscriber1Msisdn = 966544312356L
    val subscriber2Msisdn = 966565366654L
    val subscriber3Msisdn = 966565366658L
    val subscriber1Imsi = "1234567"
    val subscriber2Imsi = "456789"
    val subscriber3Imsi = "9101112"
    val subscriber1Imei = "3581870526733101"
    val subscriber2Imei = "3577590541074623"
    val subscriber3Imei = "3577590541074645"
    val customerSubscriber1 = s"$subscriber1Msisdn|Saudi National ID|1016603803|$subscriber1Imei|$subscriber1Imsi|M" +
      "|5049|3|Saudi Arabia|KSA|Pre-Paid|Voice|39.75|Retail Customer|A|Active|Siebel|Saudi Arabia|SamsungI930000" +
      "|100.050000|A|A|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val customerSubscriber2 = s"$subscriber2Msisdn|Saudi National ID|1022832941|$subscriber2Imei|$subscriber2Imsi|F" +
      "|4784|7|Saudi Arabia|KSA|Post-Paid|Voice|30.25|Large Corporate|10/24/2012|OTS|MCR|Saudi Arabia|BlackBerryQ1000" +
      "|74.590000|8/1/2014|7/23/2014|S40|55.17|26.81|60.72|64.14|112.18|6.15"
    val customerSubscriber3 = s"$subscriber3Msisdn|Saudi National ID|1022832941|$subscriber3Imei|$subscriber3Imsi|F" +
      "|4784|7|Spain|Spain|Post-Paid|Data|30.25|Large Corporate|10/24/2012|OTS|MCR|Saudi Arabia|BlackBerryQ1000" +
      "|74.590000|8/1/2014|7/23/2014|S40|55.17|26.81|60.72|64.14|112.18|6.15"
    val customerSubscriber4 = "Invalid Value|IQAMA|2363880648|||M|2302|2|Great Britain and N Ireland|" +
      "|Pre-Paid|Data|28.4166666666667|Retail Customer|5/10/2014|Active|MDM|Saudi Arabia|" +
      "|Unknown|Data|28.4166666666667|Retail Customer|5/10/2014|Active|MDM|Saudi Arabia|" +
      "SamsungGalaxyTab37.0SM-T21100|0.000000|8/1/2014||W|0|0|0|0|0|0"
    val subscriber = sc.parallelize(
      List(customerSubscriber1,
        customerSubscriber2,
        customerSubscriber3,
        customerSubscriber4))
  }

  trait WithDuplicatesSubscribers extends WithSubscriberText {

    val duplicateSubscriberImsi = "88888"
    val duplicateSubscriberStatus = "HotSIM"
    val customerSubscriber1DifferentImsiAndStatus = s"$subscriber1Msisdn|Saudi National ID|1016603803" +
      s"|$subscriber1Imei|$duplicateSubscriberImsi|M|5049|3|Saudi Arabia|KSA|Pre-Paid|Voice|39.75|Retail Customer|A" +
      s"|$duplicateSubscriberStatus|Siebel|Saudi Arabia|SamsungI930000|100.050000|A|A|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val duplicateSubscriberMsisdn = 88888
    val customerSubscriber1DifferentMsisdnAndStatus = s"$duplicateSubscriberMsisdn|Saudi National ID|1016603803" +
      s"|$subscriber1Imei|$subscriber1Imsi|M|5049|3|Saudi Arabia|KSA|Pre-Paid|Voice|39.75|Retail Customer|A" +
      s"|$duplicateSubscriberStatus|Siebel|Saudi Arabia|SamsungI930000|100.050000|A|A|S50|99.04|68.57|133.77|109.99|106.36|125.23"
  }

  trait WithSubscriberProfilingView {

    val user1 = User(
      imei = "3581870526733101",
      imsi = "420030100040377",
      msisdn = 966544312356L)
    val subscriber1 = Subscriber(
      user = user1,
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

    val user2 = User(
      imei = "",
      imsi = "420030000000002",
      msisdn = 0L)
    val subscriber2 = Subscriber(
      user = user2,
      idType =  Visa,
      idNumber = Some(1016603803l),
      age = Some(20.75f),
      gender = Female,
      siteId = Some(5049),
      regionId = Some(3),
      nationalities = Nationalities("Other", "Other"),
      types = SubscriberTypes(PostPaid, "SamsungI930000"),
      packages = SubscriberPackages(Voice, RetailCustomer),
      date = SubscriberDates(Some(1367355600000l), Some(1406840400000l), Some(1406840400000l)),
      activeStatus  = Active,
      sourceActivation = Siebel,
      roamingStatus = "Saudi Arabia",
      currentBalance = Some(100.050000f),
      calculatedSegment = S50,
      revenues = Revenues(
        m1 = 0f,
        m2 = 0f,
        m3 = 0f,
        m4 = 0f,
        m5 = 0f,
        m6 = 0f))
    val user3 = user2.copy(imsi = "420030000000003")
    val subscriber3 =
      subscriber2.copy(
        user = user3,
        age = Some(63f),
        types = subscriber2.types.copy(pay = PrePaid),
        packages = subscriber2.packages.copy(corp = LargeCorporate))
    val user4 = user2.copy(imsi = "420030000000004")
    val subscriber4 =
      subscriber2.copy(
        user = user4,
        types = subscriber2.types.copy(pay = PrePaid),
        packages = subscriber2.packages.copy(corp = RetailCustomer),
        revenues = Revenues(
          m1 = 40.04f,
          m2 = 40.57f,
          m3 = 40.77f,
          m4 = 40.99f,
          m5 = 40.36f,
          m6 = 40.23f))
    val user5 = user2.copy(imsi = "420030000000005")
    val subscriber5 =
      subscriber4.copy(
        user = user5,
        revenues = Revenues(
          m1 = 50.04f,
          m2 = 50.57f,
          m3 = 50.77f,
          m4 = 50.99f,
          m5 = 50.36f,
          m6 = 50.23f))
    val user6 = user2.copy(imsi = "420030000000006")
    val subscriber6 =
      subscriber4.copy(
        user = user6,
        revenues = Revenues(
          m1 = 60.04f,
          m2 = 60.57f,
          m3 = 60.77f,
          m4 = 60.99f,
          m5 = 60.36f,
          m6 = 60.23f))
    val user7 = user2.copy(imsi = "420030000000007")
    val subscriber7 =
      subscriber4.copy(
        user = user7,
        revenues = Revenues(
          m1 = 70.04f,
          m2 = 70.57f,
          m3 = 70.77f,
          m4 = 70.99f,
          m5 = 70.36f,
          m6 = 70.23f))
    val user8 = user2.copy(imsi = "420030000000008")
    val subscriber8 =
      subscriber4.copy(
        user = user8,
        revenues = Revenues(
          m1 = 80.04f,
          m2 = 80.57f,
          m3 = 80.77f,
          m4 = 80.99f,
          m5 = 80.36f,
          m6 = 80.23f))
    val user9 = user2.copy(imsi = "420030000000009")
    val subscriber9 =
      subscriber4.copy(
        user = user9,
        revenues = Revenues(
          m1 = 90.04f,
          m2 = 90.57f,
          m3 = 90.77f,
          m4 = 90.99f,
          m5 = 90.36f,
          m6 = 90.23f))
    val user10 = user2.copy(imsi = "420030000000010")
    val subscriber10 =
      subscriber4.copy(
        user = user10,
        revenues = Revenues(
          m1 = 100.04f,
          m2 = 100.57f,
          m3 = 100.77f,
          m4 = 100.99f,
          m5 = 100.36f,
          m6 = 100.23f))

    val userOther = user1.copy(imsi = "214341234567890")

    val subscriberNone = subscriber1.copy(user = subscriber1.user.copy(imsi = "Not Found"))

    val subscriberProfilingView1 =
      SubscriberProfilingView(
        imsi = "420030100040377",
        ageGroup = "26-60",
        genderGroup = "M",
        nationalityGroup = "Saudi Arabia",
        affluenceGroup = "Middle 30%")

    val subscriberProfilingView2 =
      SubscriberProfilingView(
        imsi = "420030000000002",
        ageGroup = "16-25",
        genderGroup = "F",
        nationalityGroup = "Non-Saudi",
        affluenceGroup = "Top 20%")
    val subscriberProfilingView3 =
      SubscriberProfilingView(
        imsi = "420030000000003",
        ageGroup = "61-",
        genderGroup = "F",
        nationalityGroup = "Non-Saudi",
        affluenceGroup = "Top 20%")
    val subscriberProfilingView4 =
      subscriberProfilingView2.copy(imsi = "420030000000004", affluenceGroup = "Bottom 50%")
    val subscriberProfilingView5 =
      subscriberProfilingView2.copy(imsi = "420030000000005", affluenceGroup = "Bottom 50%")
    val subscriberProfilingView6 =
      subscriberProfilingView2.copy(imsi = "420030000000006", affluenceGroup = "Bottom 50%")
    val subscriberProfilingView7 =
      subscriberProfilingView2.copy(imsi = "420030000000007", affluenceGroup = "Bottom 50%")
    val subscriberProfilingView8 =
      subscriberProfilingView2.copy(imsi = "420030000000008", affluenceGroup = "Bottom 50%")
    val subscriberProfilingView9 =
      subscriberProfilingView2.copy(imsi = "420030000000009", affluenceGroup = "Middle 30%")
    val subscriberProfilingView10 =
      subscriberProfilingView2.copy(imsi = "420030000000010", affluenceGroup = "Middle 30%")

    val subscriberProfilingViewOther = SubscriberProfilingView(userOther.imsi)

      subscriberProfilingView2.copy(imsi = "420030000000010", affluenceGroup = "Middle 30%")

    val users = sc.parallelize(List(user1, user2, user3, user4, user5, user6, user7, user8, user9, user10, userOther))
    val usersOther = sc.parallelize(List(userOther))
    val subscribers =
      sc.parallelize(List(subscriber1, subscriber2, subscriber3, subscriber4, subscriber5, subscriber6, subscriber7,
        subscriber8, subscriber9, subscriber10, subscriberNone))
    val subscribersProfilingView =
      sc.parallelize(List(subscriberProfilingView1, subscriberProfilingView2, subscriberProfilingView3,
        subscriberProfilingView4, subscriberProfilingView5, subscriberProfilingView6, subscriberProfilingView7,
        subscriberProfilingView8, subscriberProfilingView9, subscriberProfilingView10))

    val subscriberProfilingViewsNoInfo = sc.parallelize(List(subscriberProfilingViewOther))

    val allSubscribersProfilingView =
      sc.parallelize(List(subscriberProfilingView1, subscriberProfilingView2, subscriberProfilingView3,
        subscriberProfilingView4, subscriberProfilingView5, subscriberProfilingView6, subscriberProfilingView7,
        subscriberProfilingView8, subscriberProfilingView9, subscriberProfilingView10, subscriberProfilingViewOther))
  }

  "SubscriberDsl" should "get correctly parsed data" in new WithSubscriberText {
    subscriber.toSubscriber.count should be (3)
  }

  it should "get errors when parsing data" in new WithSubscriberText {
    subscriber.toSubscriberErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberText {
    subscriber.toParsedSubscriber.count should be (4)
  }

  it should "get the number of users by gender" in new WithSubscriberText {
    subscriber.toSubscriber.countSubscribersByGender should be(Map("M" -> 1, "F" -> 2))
  }

  it should "get the number of users by pay type" in new WithSubscriberText {
   subscriber.toSubscriber.countSubscribersByPayType should be(Map(PrePaid -> 1, PostPaid -> 2))
  }

  it should "get the number of users by data package" in new WithSubscriberText {
    subscriber.toSubscriber.countSubscribersByDataPackage should be(Map(Voice -> 2, Data -> 1))
  }

  it should "get the number of users by corporate package" in new WithSubscriberText {
    subscriber.toSubscriber.countSubscribersByCorpPackage should be(Map(LargeCorporate -> 2, RetailCustomer -> 1))
  }

  it should "get the number of users by active status" in new WithSubscriberText {
    subscriber.toSubscriber.countSubscribersByActiveStatus should be(Map(Active -> 1, Ots -> 2))
  }

  it should "get the number of users by source activation" in new WithSubscriberText {
    subscriber.toSubscriber.countSubscribersBySourceActivation should be(Map(Mcr -> 2, Siebel -> 1))
  }

  it should "get the number of users by calculated segment" in new WithSubscriberText {
    subscriber.toSubscriber.countSubscribersByCalculatedSegment should be(Map(S40 -> 2, S50 -> 1))
  }

  it should "get the number of nationalities pairs" in new WithSubscriberText {
    subscriber.toSubscriber.nationalitiesComparison.collect should contain theSameElementsAs(
      Array(
        ("SAUDI ARABIA", "SAUDI ARABIA") -> 2,
        ("SPAIN", "SPAIN") -> 1))
  }

  it should "compare calculated nationality with declared" in new WithSubscriberText {
    subscriber.toSubscriber.subscribersByMatchingNationatility.count should be(3)
  }

  it should "get the subscribers with revenues higher than the mean" in new WithSubscriberText {
    val subscribersHigherThanMean = subscriber.toSubscriber.subscribersByRevenueHigherThanMean
    subscribersHigherThanMean.count should be(1)
    subscribersHigherThanMean.first.user.msisdn should be(subscriber1Msisdn)
  }

  it should "get the subscribers with revenues lower than the mean" in new WithSubscriberText {
    val subscribersHigherThanMean = subscriber.toSubscriber.subscribersByRevenueLowerThanMean
    subscribersHigherThanMean.count should be(2)
    subscribersHigherThanMean.first.user.msisdn should be(subscriber2Msisdn)
  }

  it should "get the subscribers with revenues greater than a value" in new WithSubscriberText {
    val subscribersHigherThanMean = subscriber.toSubscriber.subscribersByRevenueGreaterThanValue(500)
    subscribersHigherThanMean.count should be(1)
  }

  it should "broadcast the user mapping by msisdn using the default method to get rid of duplicates" in new
      WithDuplicatesSubscribers {
    val subscribers = sc.parallelize(
      Array(
        customerSubscriber1,
        customerSubscriber1DifferentImsiAndStatus,
        customerSubscriber2))
    val broadcastMap = subscribers.toSubscriber.toBroadcastImsiByMsisdn()
    broadcastMap.value.size should be (2)
    broadcastMap.value(subscriber1Msisdn) should be(subscriber1Imsi)
  }

  it should "broadcast the user mapping by msisdn using a custom method to get rid of duplicates" in new
      WithDuplicatesSubscribers {
    val subscribers = sc.parallelize(
      Array(
        customerSubscriber1,
        customerSubscriber1DifferentImsiAndStatus,
        customerSubscriber2))
    val broadcastMap = subscribers.toSubscriber.toBroadcastImsiByMsisdn(
      (s1, s2) => if (s2.activeStatus==HotSIM) s2 else s1)
    broadcastMap.value.size should be (2)
    broadcastMap.value(subscriber1Msisdn) should be(duplicateSubscriberImsi)
  }

  it should "broadcast the user mapping by imsi using the default method to get rid of duplicates" in new
      WithDuplicatesSubscribers {
    val subscribers = sc.parallelize(
      Array(
        customerSubscriber1,
        customerSubscriber1DifferentMsisdnAndStatus,
        customerSubscriber2))
    val broadcastMap = subscribers.toSubscriber.toBroadcastMsisdnByImsi()
    broadcastMap.value.size should be (2)
    broadcastMap.value(subscriber1Imsi) should be(subscriber1Msisdn)
  }

  it should "broadcast the user mapping by imsi using a custom method to get rid of duplicates" in new
      WithDuplicatesSubscribers {
    val subscribers = sc.parallelize(
      Array(
        customerSubscriber1,
        customerSubscriber1DifferentMsisdnAndStatus,
        customerSubscriber2))
    val broadcastMap = subscribers.toSubscriber.toBroadcastMsisdnByImsi(
      (s1, s2) => if (s2.activeStatus==HotSIM) s2 else s1)
    broadcastMap.value.size should be (2)
    broadcastMap.value(subscriber1Imsi) should be(duplicateSubscriberMsisdn)
  }

  it should "save subscribers in parquet" in new WithSubscriberText {
    val path = File.makeTemp().name
    val subscribers = subscriber.toSubscriber
    subscribers.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriber.collect.sameElements(subscribers.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "parse to SubscriberView" in new WithSubscriberText {
    subscriber.toSubscriber.toSubscriberView.count should be (3)
  }

  it should "return correct subscribers profiling view when filtering" in new WithSubscriberProfilingView {
    val subscribersProfilingViewFiltered = subscribers.toFilteredSubscriberProfilingView(users).collect

    subscribersProfilingViewFiltered.size should be (subscribersProfilingView.count)
    subscribersProfilingViewFiltered should contain theSameElementsAs(subscribersProfilingView.collect)
  }

  it should "return correct subscriber profiling view when filtering" in new WithSubscriberProfilingView {
    val subscribersProfilingViewFiltered =
      subscribers.toFilteredSubscriberProfilingView(sc.parallelize(List(user1))).collect

    subscribersProfilingViewFiltered.size should be (1)
    subscribersProfilingViewFiltered(0) should be (subscriberProfilingView1.copy(affluenceGroup = "Top 20%"))
  }

  it should "return no subscriber profiling view when no subscriber for users" in new WithSubscriberProfilingView {
    subscribers.toFilteredSubscriberProfilingView(usersOther).count should be (0)
  }

  it should "return default subscribers profiling view when no subscriber information" in new
      WithSubscriberProfilingView {
    val subscribersProfilingViewNoSubscriberInfo =
      subscribers.toSubscriberProfilingViewNoSubsInfo(users).collect

    subscribersProfilingViewNoSubscriberInfo.size should be (subscriberProfilingViewsNoInfo.count)
    subscribersProfilingViewNoSubscriberInfo should contain theSameElementsAs(subscriberProfilingViewsNoInfo.collect)
  }

  it should "return all subscribers profiling view for users" in new
      WithSubscriberProfilingView {
    val subscribersProfilingViewResult =
      subscribers.toSubscriberProfilingView(users).collect

    subscribersProfilingViewResult.size should be (allSubscribersProfilingView.count)
    subscribersProfilingViewResult should contain theSameElementsAs(allSubscribersProfilingView.collect)
  }
}
