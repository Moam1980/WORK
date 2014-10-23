/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}

object PsEvent {

  final val lineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  implicit val fromCsv = new CsvParser[Event] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Event = {
      val (firstChunk, secondChunk) = fields.splitAt(20) // scalastyle:ignore magic.number
      val Array(msisdn, imsi, _, imei, beginTime, endTime, _, eventType, _, _, _, _, _, _, _, _, _, _, rat,
      lac) = firstChunk
      val Array(rac, sac, ci, tac, eci, _, _, _, _, _, _, _, _, _, _, _, _) = secondChunk

      Event(
        beginTime = beginTime.toLong,
        ci = ci,
        eci = eci,
        endTime = endTime.toLong,
        eventType = eventType.toShort,
        imei = imei.toLong,
        imsi = imsi.toLong,
        ixc = None,
        lac = lac,
        mcc = None,
        mnc = None,
        msisdn = msisdn.toLong,
        rac = rac,
        rat = rat.toShort,
        sac = sac,
        tac = tac)
    }
  }
}
