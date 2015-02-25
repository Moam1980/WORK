import scala.util.matching.Regex

import com.github.nscala_time.time.Imports._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.SQLContext

import sa.com.mobily.crm.spark.SubscriberDsl._
import sa.com.mobily.event.Event
import sa.com.mobily.event.spark.EventDsl._
import sa.com.mobily.utils.EdmCoreUtils

val Fmt = DateTimeFormat.forPattern("yyyy/MM/dd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
val DateRegex = "\\d{4}\\/\\d{2}\\/\\d{2}".r

def getDateFromString(dateAsString: String): DateTime = {
  Fmt.parseDateTime(dateAsString)
}

def getDateFromPath(path: String): String = DateRegex.findFirstIn(path).get

def getDateTimeFromPath(path: String): DateTime = getDateFromString(getDateFromPath(path))

def replaceDate(path: String, dateAsString: String): String = DateRegex.replaceAllIn(path, dateAsString)

def getEvents(path: String, parquetFile: SchemaRDD)(implicit bcSubscribersCatalogue: Broadcast[Map[String, Long]]): RDD[Event] = {
  if(path.contains("A-Interface")) parquetFile.toAiCsXdr.toEventWithMatchingSubscribers
  else parquetFile.toIuCsXdr.toEventWithMatchingSubscribers
}

def getDateAsStringFromDate(dt: DateTime): String = Fmt.print(dt)

def getSchema(path:String): SchemaRDD = new SQLContext(sc).parquetFile(path)

def load(path: String)(implicit bcSubscribersCatalogue: Broadcast[Map[String, Long]]): RDD[Event] = {
  val dateToParse = getDateTimeFromPath(path)
  val start = dateToParse.getMillis
  val end = dateToParse.plusDays(1).getMillis
  val events = getEvents(path, getSchema(path))
  val eventsPrevious = getEvents(path, getSchema(replaceDate(path, getDateAsStringFromDate(dateToParse.minusDays(1)))))
  val eventsNext = getEvents(path, getSchema(replaceDate(path, getDateAsStringFromDate(dateToParse.plusDays(1)))))
  events.union(eventsPrevious).union(eventsNext)
  events.filter(e=>(e.beginTime >= start && e.endTime < end))
}

val sources = List(${sources})
implicit val bcSubscribersCatalogue = sc.textFile("${subscribers}").toSubscriber.toBroadcastMsisdnByImsi()
load(sources(0)).union(load(sources(1))).saveAsParquetFile("${destination_dir}")
