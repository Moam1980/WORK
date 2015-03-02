/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.collection.immutable.Map
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.crm._
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriber: RDD[ParsedItem[Subscriber]] = SparkParser.fromCsv[Subscriber](self)

  def toSubscriber: RDD[Subscriber] = toParsedSubscriber.values

  def toSubscriberErrors: RDD[ParsingError] = toParsedSubscriber.errors
}

class SubscriberFunctions(self: RDD[Subscriber]) extends Serializable {

  def toBroadcastImsiByMsisdn(chooseSubscriber: (Subscriber, Subscriber) =>
        Subscriber = (s1, s2) => s1): Broadcast[Map[Long, String]] =
    self.sparkContext.broadcast(self.keyBy(s =>
      (s.user.msisdn)).reduceByKey(chooseSubscriber).map(e => (e._1, e._2.user.imsi)).collect.toMap)

  def toBroadcastMsisdnByImsi(chooseSubscriber: (Subscriber, Subscriber) =>
        Subscriber = (s1, s2) => s1): Broadcast[Map[String, Long]] =
    self.sparkContext.broadcast(self.keyBy(s =>
      (s.user.imsi)).reduceByKey(chooseSubscriber).map(e => (e._1, e._2.user.msisdn)).collect.toMap)

  def toSubscriberView: RDD[SubscriberView] = self.map(subscriber => SubscriberView(subscriber))
}

class SubscriberStatistics(self: RDD[Subscriber]) {

  def countSubscribers[A: ClassTag](f: Subscriber => (A, Int)): Map[A, Int] =
    self.map(f).reduceByKey(_ + _).collect.toMap

  def countSubscribersByGender: Map[String, Int] = countSubscribers(e => (e.gender, 1))

  def countSubscribersByPayType: Map[PayType, Int] = countSubscribers(e => (e.types.pay, 1))

  def countSubscribersByDataPackage: Map[DataPackage, Int] = countSubscribers(e => (e.packages.data, 1))

  def countSubscribersByCorpPackage: Map[CorpPackage, Int] = countSubscribers(e => (e.packages.corp, 1))

  def countSubscribersByActiveStatus: Map[ActiveStatus, Int] = countSubscribers(e => (e.activeStatus, 1))

  def countSubscribersBySourceActivation: Map[SourceActivation, Int] = countSubscribers(e => (e.sourceActivation, 1))

  def countSubscribersByCalculatedSegment: Map[CalculatedSegment, Int] =
    countSubscribers(e => (e.calculatedSegment, 1))

  def nationalitiesComparison: RDD[((String, String), Int)] =
    self.map(e => ((e.nationalities.declared, e.nationalities.inferred), 1)).reduceByKey(_ + _)

  def subscribersByMatchingNationatility: RDD[Subscriber] =
    self.filter(e => e.nationalities.declared == e.nationalities.inferred)

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

class SubscriberWriter(self: RDD[Subscriber]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Subscriber](self, path)
}

class SubscriberRowReader(self: RDD[Row]) {

  def toSubscriber: RDD[Subscriber] = SparkParser.fromRow[Subscriber](self)
}

trait SubscriberDsl {

  implicit def subscriberCsvReader(csv: RDD[String]): SubscriberCsvReader = new SubscriberCsvReader(csv)

  implicit def customerSubscriberStatistics(subscribers: RDD[Subscriber]): SubscriberStatistics =
    new SubscriberStatistics(subscribers)

  implicit def customerSubscriberFunctions(subscribers: RDD[Subscriber]): SubscriberFunctions =
    new SubscriberFunctions(subscribers)

  implicit def subscriberWriter(self: RDD[Subscriber]): SubscriberWriter = new SubscriberWriter(self)

  implicit def subscriberRowReader(self: RDD[Row]): SubscriberRowReader = new SubscriberRowReader(self)
}

object SubscriberDsl extends SubscriberDsl with ParsedItemsDsl
