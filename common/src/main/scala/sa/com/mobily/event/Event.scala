/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import scala.language.existentials

import com.github.nscala_time.time.Imports.DateTimeFormat
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql._
import org.joda.time.format.DateTimeFormatter

import sa.com.mobily.cell.Cell
import sa.com.mobily.metrics.{MeasurableById, MeasurableByTime, MeasurableByType}
import sa.com.mobily.parsing.{OpenCsvParser, RowParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

/** Source of the event */
sealed case class EventSource(id: String)

object CsSmsSource extends EventSource(id = "CS.SMS")
object CsVoiceSource extends EventSource(id = "CS.Voice")
object CsAInterfaceSource extends EventSource(id = "Cs.A-Interface")
object CsIuSource extends EventSource(id = "CS.Iu")
object PsEventSource extends EventSource(id = "PS.Event")
object PsUfdrSource extends EventSource(id = "PS.UFDR")

case class Event(
    user: User,
    beginTime: Long,
    endTime: Long,
    lacTac: Int,
    cellId: Int,
    source: EventSource,
    eventType: Option[String] = None, // TODO concrete types
    subsequentLacTac: Option[Int],
    subsequentCellId: Option[Int],
    inSpeed: Option[Double] = None,
    outSpeed: Option[Double] = None,
    minSpeedPointWkt: Option[String] = None) extends MeasurableByTime with MeasurableByType with MeasurableById[Long] {

  override def typeValue: String = source.id + "." + eventType.getOrElse(NonDefined)

  override def id: Long = user.id

  override def timeValue: Long = beginTime

  def minSpeedPopulated: Boolean = inSpeed.isDefined && outSpeed.isDefined && minSpeedPointWkt.isDefined

  def fields: Array[String] =
    user.fields ++ Array(beginTime.toString, endTime.toString, lacTac.toString, cellId.toString, source.id,
      eventType.getOrElse(""), subsequentLacTac.getOrElse("").toString, subsequentCellId.getOrElse("").toString,
      inSpeed.getOrElse("").toString, outSpeed.getOrElse("").toString, minSpeedPointWkt.getOrElse(""))
}

object Event {

  val HoursInWeek = 168
  val HoursInDay = 24
  val LineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  val DateFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS").withZone(EdmCoreUtils.TimeZoneSaudiArabia)

  def lacOrTac(lac: String, tac: String): String = if (lac.isEmpty) tac else lac

  def sacOrCi(sac: String, ci: String): String = if (sac.isEmpty) ci else sac

  implicit val fromRow = new RowParser[Event] {

    override def fromRow(row: Row): Event = {
      val Seq(Seq(imei, imsi, msisdn), beginTime, endTime, lacTac, cellId, Seq(source), eventType, subsequentLacTac,
        subsequentCellId, inSpeed, outSpeed, minSpeedPointWkt) = row.toSeq

      Event(
        user =
          User(imei = imei.asInstanceOf[String], imsi = imsi.asInstanceOf[String], msisdn = msisdn.asInstanceOf[Long]),
        beginTime = beginTime.asInstanceOf[Long],
        endTime = endTime.asInstanceOf[Long],
        lacTac = lacTac.asInstanceOf[Int],
        cellId = cellId.asInstanceOf[Int],
        source = parseEventSource(source.asInstanceOf[String]),
        eventType = EdmCoreUtils.stringOption(eventType),
        subsequentLacTac = EdmCoreUtils.intOption(subsequentLacTac),
        subsequentCellId = EdmCoreUtils.intOption(subsequentCellId),
        inSpeed = EdmCoreUtils.doubleOption(inSpeed),
        outSpeed = EdmCoreUtils.doubleOption(outSpeed),
        minSpeedPointWkt = EdmCoreUtils.stringOption(minSpeedPointWkt))
    }
  }

  def geom(cells: Map[(Int, Int), Cell])(event: Event): Geometry = cells((event.lacTac, event.cellId)).coverageGeom

  def geomWkt(cells: Map[(Int, Int), Cell])(event: Event): String = cells((event.lacTac, event.cellId)).coverageWkt

  def header: Array[String] =
    User.header ++ Array("beginTime", "endTime", "lacTac", "cellId", "source", "eventType",
      "subsequentLacTac", "subsequentCellId", "inSpeed", "outSpeed", "minSpeedPointWkt")

  def parseEventSource(sourceText: String): EventSource = sourceText match {
    case CsSmsSource.id => CsSmsSource
    case CsVoiceSource.id => CsVoiceSource
    case CsAInterfaceSource.id => CsAInterfaceSource
    case CsIuSource.id => CsIuSource
    case PsEventSource.id => PsEventSource
    case PsUfdrSource.id => PsUfdrSource
  }
}
