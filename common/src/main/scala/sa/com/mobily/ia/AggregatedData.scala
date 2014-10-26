/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class AggregatedData(
    timestamp: Long,
    subscriberCount: Long,
    urlValidCount: Long,
    urlCrawlCount: Long,
    urlIndexCount: Long,
    uploadVolume: Double,
    downloadVolume: Double,
    totalVolume: Double,
    keywordSearchCount: Long,
    keywordContentCount: Long,
    businessEntityId: String)

trait IaParser {

  val inputDateTimeFormat = "yyyyMMdd"
  final val fmt = DateTimeFormat.forPattern(inputDateTimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
  final val lineCsvParserObject = new OpenCsvParser(quote = '"')
}

object AggregatedData extends IaParser {

  implicit val fromCsv = new CsvParser[AggregatedData] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): AggregatedData = {
      val Array(dateText, subscriberCountText, urlValidCountText, urlCrawlCountText, urlIndexCountText,
        uploadVolumeText, downloadVolumeText, totalVolumeText, keywordSearchCountText, keywordContentCountText,
        businessEntityIdText) = fields

      AggregatedData(
        timestamp = fmt.parseDateTime(dateText).getMillis,
        subscriberCount = subscriberCountText.toLong,
        urlValidCount = urlValidCountText.toLong,
        urlCrawlCount = urlCrawlCountText.toLong,
        urlIndexCount = urlIndexCountText.toLong,
        uploadVolume = uploadVolumeText.toDouble,
        downloadVolume = downloadVolumeText.toDouble,
        totalVolume = totalVolumeText.toDouble,
        keywordSearchCount = keywordSearchCountText.toLong,
        keywordContentCount = keywordContentCountText.toLong,
        businessEntityId = businessEntityIdText)
    }
  }
}
