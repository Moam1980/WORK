/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class SubscriberIaApps(
    timestamp: Long,
    subscriberId: String,
    appType: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String) {

  def fields: Array[String] =
    Array(AggregatedData.Fmt.print(timestamp), subscriberId, appType) ++
      trafficInfo.fields ++
      Array(locationId, businessEntityId)
}

object SubscriberIaApps extends IaParser {

  val Header: Array[String] =
    Array("date", "subscriberId", "appType") ++
      TrafficInfo.Header ++
      Array("locationId", "businessEntityId")

  implicit val fromCsv = new CsvParser[SubscriberIaApps] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaApps = {
      val Array(dateText, subscriberIdText, appTypeText, visitCountText, uploadVolumeText, downloadVolumeText,
        totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaApps(
        timestamp = Fmt.parseDateTime(dateText).getMillis,
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

  implicit val fromRow = new RowParser[SubscriberIaApps] {

    override def fromRow(row: Row): SubscriberIaApps = {
      val Row(
        timestamp,
        subscriberId,
        appType,
        trafficInfo,
        locationId,
        businessEntityId) = row

      SubscriberIaApps(
        timestamp = timestamp.asInstanceOf[Long],
        subscriberId = subscriberId.asInstanceOf[String],
        appType = appType.asInstanceOf[String],
        trafficInfo = TrafficInfo.fromRow.fromRow(trafficInfo.asInstanceOf[Row]),
        locationId = locationId.asInstanceOf[String],
        businessEntityId = businessEntityId.asInstanceOf[String])
    }
  }
}
