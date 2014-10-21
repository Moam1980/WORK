/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.event.{CsEvent, Event, PsEvent}
import sa.com.mobily.parsing.spark.{ParsedItemsContext, SparkCsvParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class EventReader(csEvents: RDD[CsEvent], psEvents: RDD[PsEvent]) {

  def toEvent: RDD[Event] = {
    fromCsEventToEvent.union(fromPsEventToEvent)
  }

  def fromCsEventToEvent: RDD[Event] = {
    csEvents.map(c => Event(c.beginTime, c.ci, c.eci, c.endTime, c.eventType, c.imei, c.imsi, c.ixc, c.lac, c.mcc,
      c.mnc, c.msisdn, c.rac, c.rat, c.sac, c.tac))
  }

  def fromPsEventToEvent: RDD[Event] = {
    psEvents.map(p => Event(p.beginTime, p.ci, p.eci, p.endTime, p.eventType, p.imei, p.imsi, p.ixc, p.lac, p.mcc,
      p.mnc, p.msisdn, p.rac, p.rat, p.sac, p.tac))
  }

}

class CsEventReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsContext._

  def toCsEvent: RDD[CsEvent] = toParsedCsEvent.values

  def toCsEventErrors: RDD[ParsingError] = toParsedCsEvent.errors

  def toParsedCsEvent: RDD[ParsedItem[CsEvent]] = SparkCsvParser.fromCsv[CsEvent](self)
}

class PsEventReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsContext._

  def toPsEvent: RDD[PsEvent] = toParsedPsEvent.values

  def toPsEventErrors: RDD[ParsingError] = toParsedPsEvent.errors

  def toParsedPsEvent: RDD[ParsedItem[PsEvent]] = SparkCsvParser.fromCsv[PsEvent](self)
}

trait EventContext {

  implicit def eventReader(csEvents: RDD[CsEvent], psEvents: RDD[PsEvent]): EventReader = new EventReader(csEvents,
    psEvents)
}

trait CsEventContext {

  implicit def csEventReader(csv: RDD[String]): CsEventReader = new CsEventReader(csv)
}

trait PsEventContext {

  implicit def psEventReader(csv: RDD[String]): PsEventReader = new PsEventReader(csv)
}

object EventContext extends EventContext with CsEventContext with PsEventContext with ParsedItemsContext
