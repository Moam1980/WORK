/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class SubscriberIaApps(
    timestamp: Long,
    subscriberId: String,
    appType: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String)

object SubscriberIaApps extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIaApps] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaApps = {
      val Array(dateText, subscriberIdText, appTypeText, visitCountText, uploadVolumeText, downloadVolumeText,
        totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaApps(
        timestamp = fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        appType = appTypeText,
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
