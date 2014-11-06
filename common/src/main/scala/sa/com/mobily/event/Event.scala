/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import com.github.nscala_time.time.Imports.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

import sa.com.mobily.parsing.OpenCsvParser
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
    subsequentCellId: Option[Int],
    inSpeed: Option[Double] = None,
    outSpeed: Option[Double] = None)

object Event {

  val LineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  val DateFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS").withZone(TimeZoneSaudiArabia)

  def lacOrTac(lac: String, tac: String): String = if (lac.isEmpty) tac else lac

  def sacOrCi(sac: String, ci: String): String = if (sac.isEmpty) ci else sac
}
