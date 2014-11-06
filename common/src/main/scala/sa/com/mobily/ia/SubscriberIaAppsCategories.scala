/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class SubscriberIaAppsCategories(
    timestamp: Long,
    subscriberId: String,
    appGroup: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String)

object SubscriberIaAppsCategories extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIaAppsCategories] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaAppsCategories = {
      val Array(dateText, subscriberIdText, appGroupText, visitCountText, uploadVolumeText, downloadVolumeText,
      totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaAppsCategories(
        timestamp = fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        appGroup = appGroupText,
        trafficInfo = TrafficInfo(
          visitCount = visitCountText.toLong,
          uploadVolume = uploadVolumeText.toDouble,
          downloadVolume = downloadVolumeText.toDouble,
          totalVolume = totalVolumeText.toDouble),
        locationId = locationIdText,
        businessEntityId = businessEntityIdText)
    }
  }
}
