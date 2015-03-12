import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import sa.com.mobily.crm.spark.SubscriberDsl._
import sa.com.mobily.event.Event
import sa.com.mobily.event.spark.EventDsl._
import sa.com.mobily.utils.spark.RddRowDsl
import sa.com.mobily.utils.spark.RddRowDsl._
import sa.com.mobily.xdr.spark.AiCsXdrDsl._
import sa.com.mobily.xdr.spark.IuCsXdrDsl._

val sources = List(${sources})
implicit val bcSubscribersCatalogue: Broadcast[Map[String,Long]] = new SQLContext(sc).parquetFile("${subscribers}").toSubscriber.toBroadcastMsisdnByImsi()

def eventsFromCs(path: String, rdd: RDD[Row]): RDD[Event] = {
  val AiCs = """.*(A-Interface){1}.*""".r
  val IuCs = """.*(IUCS-Interface){1}.*""".r
  path match {
    case AiCs(c) => rdd.toAiCsXdr.toEvent
    case IuCs(c) => rdd.toIuCsXdr.toEvent
  }
}

def events(path: String): RDD[Event] = {
  val dt = RddRowDsl.getDateFromPath(path).get
  val start = dt.minusDays(1)
  val end = dt.plusDays(1)
  val rdd = new SQLContext(sc).readFromPeriod(path,start,end)
  eventsFromCs(path,rdd).filter(e => e.beginTime >= dt.getMillis && e.beginTime < dt.plusDays(1).getMillis)
}

def load(sources: List[String], result: RDD[Event] = sc.emptyRDD): RDD[Event] = {
  if (sources.size == 1) result.union(events(sources.head))
  else load(sources.drop(1), result.union(events(sources.head)))
}

load(sources).saveAsParquetFile("${destination_dir}")
