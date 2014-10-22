/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class SubscriberIaDomainsCategories(
    timestamp: Long,
    subscriberId: String,
    categoryId: String,
    visitCount: Long,
    uploadVolume: Double,
    downloadVolume: Double,
    totalVolume: Double,
    locationId: String,
    businessEntityId: String)

object SubscriberIaDomainsCategories extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIaDomainsCategories] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaDomainsCategories = {
      val Array(dateText, subscriberIdText, categoryIdText, visitCountText, uploadVolumeText, downloadVolumeText,
      totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaDomainsCategories(
        timestamp = fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        categoryId = categoryIdText,
        visitCount = visitCountText.toLong,
        uploadVolume = uploadVolumeText.toDouble,
        downloadVolume = downloadVolumeText.toDouble,
        totalVolume = totalVolumeText.toDouble,
        locationId = locationIdText,
        businessEntityId = businessEntityIdText)
    }
  }
}
