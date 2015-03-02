/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class SubscriberIaDomains(
    timestamp: Long,
    subscriberId: String,
    domainName: String,
    secondLevelDomain: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String) {

  def fields: Array[String] =
    Array(AggregatedData.Fmt.print(timestamp), subscriberId, domainName, secondLevelDomain) ++
      trafficInfo.fields ++
      Array(locationId, businessEntityId)
}

object SubscriberIaDomains extends IaParser {

  val Header: Array[String] =
    Array("date", "subscriberId", "domainName", "secondLevelDomain") ++
      TrafficInfo.Header ++
      Array("locationId", "businessEntityId")

  implicit val fromCsv = new CsvParser[SubscriberIaDomains] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaDomains = {
      val Array(dateText, subscriberIdText, domainNameText, secondLevelDomainText, visitCountText, uploadVolumeText,
        downloadVolumeText, totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaDomains(
        timestamp = Fmt.parseDateTime(dateText).getMillis,
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

  implicit val fromRow = new RowParser[SubscriberIaDomains] {

    override def fromRow(row: Row): SubscriberIaDomains = {
      val Row(
        timestamp,
        subscriberId,
        domainName,
        secondLevelDomain,
        trafficInfo,
        locationId,
        businessEntityId) = row

      SubscriberIaDomains(
        timestamp = timestamp.asInstanceOf[Long],
        subscriberId = subscriberId.asInstanceOf[String],
        domainName = domainName.asInstanceOf[String],
        secondLevelDomain = secondLevelDomain.asInstanceOf[String],
        trafficInfo = TrafficInfo.fromRow.fromRow(trafficInfo.asInstanceOf[Row]),
        locationId = locationId.asInstanceOf[String],
        businessEntityId = businessEntityId.asInstanceOf[String])
    }
  }
}
