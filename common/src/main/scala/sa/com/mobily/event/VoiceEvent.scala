/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

object VoiceEvent {

  implicit val fromCsv = new CsvParser[Event] {

    override def lineCsvParser: OpenCsvParser = Event.LineCsvParserObject

    override def fromFields(fields: Array[String]): Event = {
      val (firstChunk, remaining) = fields.splitAt(20) // scalastyle:ignore magic.number
      val (secondChunk, remaining2) = remaining.splitAt(18) // scalastyle:ignore magic.number
      val (thirdChunk, remaining3) = remaining2.splitAt(22) // scalastyle:ignore magic.number
      val (fourthChunk, fifthChunk) = remaining3.splitAt(20) // scalastyle:ignore magic.number
      val Array(_, _, beginTime, endTime, _, _, _, _, _, _, _, _, _, _, _, imei, tac, imsi, _, _) = firstChunk
      val Array(msisdn, _, _, _, _, _, _, _, firstCellId, lastCellId, firstLac, lastLac, _, _, _, _, _, _) = secondChunk
      val Array(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) = thirdChunk
      val Array(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, eventType, _, _, _, _) = fourthChunk
      val Array(_, _, _, _, _, _, _, _, _, _, _) = fifthChunk
      val user: User = User(
        imei = imei,
        imsi = imsi,
        msisdn = msisdn.toLong)

      Event(
        user = user,
        beginTime = Event.DateFormatter.parseDateTime(beginTime).getMillis,
        endTime = Event.DateFormatter.parseDateTime(endTime).getMillis,
        lacTac = Event.lacOrTac(firstLac, tac).toInt,
        cellId = firstCellId.toInt,
        eventType = EdmCoreUtils.parseString(eventType),
        subsequentLacTac = EdmCoreUtils.parseInt(lastLac),
        subsequentCellId = EdmCoreUtils.parseInt(lastCellId))
    }
  }
}
