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

case class Event(
    user: User,
    beginTime: Long,
    endTime: Long,
    lacTac: Int,
    cellId: Int,
    eventType: String, // TODO concrete types
    subsequentLacTac: Option[Int],
    subsequentCellId: Option[Int],
    inSpeed: Option[Double] = None,
    outSpeed: Option[Double] = None,
    minSpeedPointWkt: Option[String] = None) extends MeasurableByTime with MeasurableByType with MeasurableById[Long] {

  override def typeValue: String = eventType

  override def id: Long = user.id

  override def timeValue: Long = beginTime

  def minSpeedPopulated: Boolean = inSpeed.isDefined && outSpeed.isDefined && minSpeedPointWkt.isDefined

  lazy val regionId: Short = lacTac.toString.head.toShort
}

object Event {

  val HoursInWeek = 168
  val HoursInDay = 24
  val DefaultMinActivityRatio = 0.1
  val LineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  val DateFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS").withZone(EdmCoreUtils.TimeZoneSaudiArabia)

  def lacOrTac(lac: String, tac: String): String = if (lac.isEmpty) tac else lac

  def sacOrCi(sac: String, ci: String): String = if (sac.isEmpty) ci else sac

  implicit val fromRow = new RowParser[Event] {

    override def fromRow(row: Row): Event = {
      val Seq(Seq(imei, imsi, msisdn), beginTime, endTime, lacTac, cellId, eventType, subsequentLacTac,
        subsequentCellId, inSpeed, outSpeed, minSpeedPointWkt) = row.toSeq

      Event(
        user =
          User(imei = imei.asInstanceOf[String], imsi = imsi.asInstanceOf[String], msisdn = msisdn.asInstanceOf[Long]),
        beginTime = beginTime.asInstanceOf[Long],
        endTime = endTime.asInstanceOf[Long],
        lacTac = lacTac.asInstanceOf[Int],
        cellId = cellId.asInstanceOf[Int],
        eventType = eventType.asInstanceOf[String],
        subsequentLacTac = EdmCoreUtils.intOption(subsequentLacTac),
        subsequentCellId = EdmCoreUtils.intOption(subsequentCellId),
        inSpeed = EdmCoreUtils.doubleOption(inSpeed),
        outSpeed = EdmCoreUtils.doubleOption(outSpeed),
        minSpeedPointWkt = EdmCoreUtils.stringOption(minSpeedPointWkt))
    }
  }

  def geom(cells: Map[(Int, Int), Cell])(event: Event): Geometry = cells((event.lacTac, event.cellId)).coverageGeom

  def geomWkt(cells: Map[(Int, Int), Cell])(event: Event): String = cells((event.lacTac, event.cellId)).coverageWkt
}
