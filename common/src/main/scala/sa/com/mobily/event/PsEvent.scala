/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils._

object PsEvent {


  implicit val fromCsv = new CsvParser[Event] {

    override def lineCsvParser: OpenCsvParser = Event.LineCsvParserObject

    override def fromFields(fields: Array[String]): Event = {
      val (firstChunk, secondChunk) = fields.splitAt(19) // scalastyle:ignore magic.number
      val Array(msisdn, imsi, _, imei, beginTime, endTime, _, eventType, _, _, _, _, _, _, _, _, _, _, _) = firstChunk
      val Array(lac, _, sac, ci, tac, eci, _, _, _, _, _, _, _, _, _, _, _, _) = secondChunk
      val user: User = User(imei = imei, imsi = imsi, msisdn = msisdn.toLong)

      Event(
        user = user,
        beginTime = beginTime.toLong * 1000,
        endTime = endTime.toLong * 1000,
        lacTac = Integer.parseInt(Event.lacOrTac(lac, tac), BaseForHexadecimal),
        cellId = Integer.parseInt(Event.sacOrCi(sac, ci), BaseForHexadecimal),
        eventType = eventType,
        subsequentLacTac = None,
        subsequentCellId = None)
    }
  }
}
