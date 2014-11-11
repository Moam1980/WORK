/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import com.github.nscala_time.time.Imports.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.spark.sql._

import sa.com.mobily.parsing.{RowParser, OpenCsvParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils._

case class Event(
    user: User,
    beginTime: Long,
    endTime: Long,
    lacTac: Int,
    cellId: Int,
    eventType: String, // TODO concrete types
    subsequentLacTac: Option[Int],
    subsequentCellId: Option[Int])

object Event {

  val LineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  val DateFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS").withZone(TimeZoneSaudiArabia)

  def lacOrTac(lac: String, tac: String): String = if (lac.isEmpty) tac else lac

  def sacOrCi(sac: String, ci: String): String = if (sac.isEmpty) ci else sac

  implicit val fromRow = new RowParser[Event] {
    override def fromRow(row: Row): Event = {
      val Array(imei, imsi, msisdn, beginTime, endTime, lacTac, cellId, eventType, subsequentLacTac, subsequentCellId) =
        row.toSeq.toArray
      Event(
        user =
          User(imei = imei.asInstanceOf[String], imsi = imsi.asInstanceOf[String], msisdn = msisdn.asInstanceOf[Long]),
        beginTime = beginTime.asInstanceOf[Long],
        endTime = endTime.asInstanceOf[Long],
        lacTac = lacTac.asInstanceOf[Int],
        cellId = cellId.asInstanceOf[Int],
        eventType = eventType.asInstanceOf[String],
        subsequentLacTac = Option(subsequentLacTac.asInstanceOf[Int]),
        subsequentCellId = Option(subsequentCellId.asInstanceOf[Int]))
    }
  }
}
