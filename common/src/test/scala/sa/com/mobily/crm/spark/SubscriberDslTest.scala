/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.reflect.io.File

import org.scalatest._

import sa.com.mobily.crm._
import sa.com.mobily.utils.LocalSparkSqlContext

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

  "SubscriberDsl" should "get correctly parsed data" in new WithSubscriberText {
    val subscribers = subscriber.toSubscriber.collect
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
    subscriber.toSubscriber.nationalitiesComparison.collect should be(
      Array(
        ("SAUDI ARABIA", "KSA") -> 2,
        ("SPAIN", "SPAIN") -> 1))
  }

  it should "compare calculated nationality with declared" in new WithSubscriberText {
    subscriber.toSubscriber.subscribersByMatchingNationatility.count should be(1)
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
}
