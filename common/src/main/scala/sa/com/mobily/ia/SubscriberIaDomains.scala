/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class SubscriberIaDomains(
    timestamp: Long,
    subscriberId: String,
    domainName: String,
    secondLevelDomain: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String)

object SubscriberIaDomains extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIaDomains] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaDomains = {
      val Array(dateText, subscriberIdText, domainNameText, secondLevelDomainText, visitCountText, uploadVolumeText,
        downloadVolumeText, totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaDomains(
        timestamp = fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        domainName = domainNameText,
        secondLevelDomain = secondLevelDomainText,
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
