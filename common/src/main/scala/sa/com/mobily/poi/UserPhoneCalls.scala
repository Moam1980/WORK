/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.collection.Seq

import com.github.nscala_time.time.Imports._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class UserPhoneCalls(
    msisdn: Long,
    timestamp: DateTime,
    siteId: String,
    regionId: Long,
    callHours: Seq[Int])

object UserPhoneCalls {

  final val UserPhoneCallSeparator = ","
  final val lineCsvParserObject = new OpenCsvParser(separator = ',', quote = '\'')

  implicit val fromCsv = new CsvParser[UserPhoneCalls] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): UserPhoneCalls = {
      val Array(msisdn, timestamp, siteId, regionId, callHours) = fields

      UserPhoneCalls(
        msisdn = msisdn.toLong,
        timestamp =
          DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime(timestamp),
        siteId = siteId,
        regionId = regionId.toLong,
        callHours = callHours.split(UserPhoneCallSeparator).map(hs => hs.trim.toInt))
    }
  }
}
