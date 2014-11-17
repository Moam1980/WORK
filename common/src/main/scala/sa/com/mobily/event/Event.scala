/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import scala.language.existentials

import com.github.nscala_time.time.Imports.DateTimeFormat
import org.apache.spark.sql._
import org.joda.time.format.DateTimeFormatter

import sa.com.mobily.metrics.{MeasurableByTime, MeasurableByType}
import sa.com.mobily.parsing.{OpenCsvParser, RowParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils
import sa.com.mobily.utils.EdmCoreUtils._

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
    outSpeed: Option[Double] = None) extends MeasurableByTime with MeasurableByType {

  override def typeValue: String = eventType

  override def timeValue: Long = beginTime
}

object Event {

  val LineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  val DateFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS").withZone(TimeZoneSaudiArabia)

  def lacOrTac(lac: String, tac: String): String = if (lac.isEmpty) tac else lac

  def sacOrCi(sac: String, ci: String): String = if (sac.isEmpty) ci else sac

  implicit val fromRow = new RowParser[Event] {
    override def fromRow(row: Row): Event = {
      val Seq(Seq(imei, imsi, msisdn),
        beginTime, endTime, lacTac, cellId, eventType, subsequentLacTac,subsequentCellId, inSpeed, outSpeed) = row.toSeq
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
        outSpeed = EdmCoreUtils.doubleOption(outSpeed))
    }
  }
}
