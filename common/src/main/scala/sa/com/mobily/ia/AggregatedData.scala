/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._
import org.joda.time.format.DateTimeFormat

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
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
    businessEntityId: String) {

  def fields: Array[String] =
    Array(AggregatedData.Fmt.print(timestamp),
      subscriberCount.toString,
      urlValidCount.toString,
      urlCrawlCount.toString,
      urlIndexCount.toString,
      uploadVolume.toString,
      downloadVolume.toString,
      totalVolume.toString,
      keywordSearchCount.toString,
      keywordContentCount.toString,
      businessEntityId)
}

trait IaParser {

  val inputDateTimeFormat = "yyyyMMdd"
  final val Fmt = DateTimeFormat.forPattern(inputDateTimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
  final val lineCsvParserObject = new OpenCsvParser(quote = '"')
}

object AggregatedData extends IaParser {

  val Header: Array[String] =
    Array("date", "subscriberCount", "urlValidCount", "urlCrawlCount", "urlIndexCount", "uploadVolume",
      "downloadVolume", "totalVolume", "keywordSearchCount", "keywordContentCount", "businessEntityId")

  implicit val fromCsv = new CsvParser[AggregatedData] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): AggregatedData = {
      val Array(dateText, subscriberCountText, urlValidCountText, urlCrawlCountText, urlIndexCountText,
        uploadVolumeText, downloadVolumeText, totalVolumeText, keywordSearchCountText, keywordContentCountText,
        businessEntityIdText) = fields

      AggregatedData(
        timestamp = Fmt.parseDateTime(dateText).getMillis,
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

  implicit val fromRow = new RowParser[AggregatedData] {

    override def fromRow(row: Row): AggregatedData = {
      val Row(
        timestamp,
        subscriberCount,
        urlValidCount,
        urlCrawlCount,
        urlIndexCount,
        uploadVolume,
        downloadVolume,
        totalVolume,
        keywordSearchCount,
        keywordContentCount,
        businessEntityId) = row

      AggregatedData(
        timestamp = timestamp.asInstanceOf[Long],
        subscriberCount = subscriberCount.asInstanceOf[Long],
        urlValidCount = urlValidCount.asInstanceOf[Long],
        urlCrawlCount = urlCrawlCount.asInstanceOf[Long],
        urlIndexCount = urlIndexCount.asInstanceOf[Long],
        uploadVolume = uploadVolume.asInstanceOf[Double],
        downloadVolume = downloadVolume.asInstanceOf[Double],
        totalVolume = totalVolume.asInstanceOf[Double],
        keywordSearchCount = keywordSearchCount.asInstanceOf[Long],
        keywordContentCount = keywordContentCount.asInstanceOf[Long],
        businessEntityId = businessEntityId.asInstanceOf[String])
    }
  }
}
