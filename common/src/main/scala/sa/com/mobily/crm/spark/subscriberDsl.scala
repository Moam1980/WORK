/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.collection.Map
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.crm._
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class SubscriberReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriber: RDD[ParsedItem[Subscriber]] = SparkParser.fromCsv[Subscriber](self)

  def toSubscriber: RDD[Subscriber] = toParsedSubscriber.values

  def toSubscriberErrors: RDD[ParsingError] = toParsedSubscriber.errors
}

trait SubscriberDsl {

  implicit def customerSubscriberReader(csv: RDD[String]): SubscriberReader = new SubscriberReader(csv)

  implicit def customerSubscriberStatistics(subscribers: RDD[Subscriber]): SubscriberStatistics =
    new SubscriberStatistics(subscribers)

  implicit def customerSubscriberFunctions(subscribers: RDD[Subscriber]): SubscriberFunctions =
    new SubscriberFunctions(subscribers)
}

class SubscriberFunctions(self: RDD[Subscriber]) extends Serializable {

  def toBroadcastImsiByMsisdn(
      chooseSubscriber: (Subscriber, Subscriber) => Subscriber = (s1, s2) => s1): Broadcast[Map[Long, String]] =
    self.sparkContext.broadcast(
      self.keyBy(s => (s.user.msisdn)).reduceByKey(chooseSubscriber).map(e => (e._1, e._2.user.imsi)).collectAsMap)

  def toBroadcastMsisdnByImsi(
      chooseSubscriber: (Subscriber, Subscriber) => Subscriber = (s1, s2) => s1): Broadcast[Map[String, Long]] =
    self.sparkContext.broadcast(
      self.keyBy(s => (s.user.imsi)).reduceByKey(chooseSubscriber).map(e => (e._1, e._2.user.msisdn)).collectAsMap)
}

class SubscriberStatistics(self: RDD[Subscriber]) {

  def countSubscribers[A: ClassTag](f: Subscriber => (A, Int)): Map[A, Int] =
    self.map(f).reduceByKey(_ + _).collectAsMap

  def countSubscribersByGender: Map[String, Int] = countSubscribers(e => (e.gender, 1))

  def countSubscribersByPayType: Map[PayType, Int] = countSubscribers(e => (e.types.pay, 1))

  def countSubscribersByDataPackage: Map[DataPackage, Int] = countSubscribers(e => (e.packages.data, 1))

  def countSubscribersByCorpPackage: Map[CorpPackage, Int] = countSubscribers(e => (e.packages.corp, 1))

  def countSubscribersByActiveStatus: Map[ActiveStatus, Int] = countSubscribers(e => (e.activeStatus, 1))

  def countSubscribersBySourceActivation: Map[SourceActivation, Int] = countSubscribers(e => (e.sourceActivation, 1))

  def countSubscribersByCalculatedSegment: Map[CalculatedSegment, Int] =
    countSubscribers(e => (e.m1CalculatedSegment, 1))

  def nationalitiesComparison: RDD[((String, String), Int)] =
    self.map(e => ((e.nationalies.declared, e.nationalies.inferred), 1)).reduceByKey(_ + _)

  def subscribersByMatchingNationatility: RDD[Subscriber] =
    self.filter(e => e.nationalies.declared == e.nationalies.inferred)

  def subscribersByRevenueHigherThanMean: RDD[Subscriber] = {
    val subscribersRevenueMean = self.map(e => e.revenues.totalRevenue).mean
    self.filter(subscriber => subscriber.revenues.totalRevenue > subscribersRevenueMean)
  }

  def subscribersByRevenueLowerThanMean: RDD[Subscriber] = {
    val subscribersRevenueMean = self.map(e => e.revenues.totalRevenue).mean
    self.filter(subscriber => subscriber.revenues.totalRevenue < subscribersRevenueMean)
  }

  def subscribersByRevenueGreaterThanValue(value: Long): RDD[Subscriber] =
    self.filter(subscriber => subscriber.revenues.totalRevenue > value)
}

object SubscriberDsl extends SubscriberDsl with ParsedItemsDsl
