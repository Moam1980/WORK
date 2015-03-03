/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class SubscriberIaAppsCategories(
    timestamp: Long,
    subscriberId: String,
    appGroup: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String) {

  def fields: Array[String] =
    Array(AggregatedData.Fmt.print(timestamp), subscriberId, appGroup) ++
      trafficInfo.fields ++
      Array(locationId, businessEntityId)
}

object SubscriberIaAppsCategories extends IaParser {

  val Header: Array[String] =
    Array("date", "subscriberId", "appGroup") ++
      TrafficInfo.Header ++
      Array("locationId", "businessEntityId")

  implicit val fromCsv = new CsvParser[SubscriberIaAppsCategories] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaAppsCategories = {
      val Array(dateText, subscriberIdText, appGroupText, visitCountText, uploadVolumeText, downloadVolumeText,
      totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaAppsCategories(
        timestamp = Fmt.parseDateTime(dateText).getMillis,
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

  implicit val fromRow = new RowParser[SubscriberIaAppsCategories] {

    override def fromRow(row: Row): SubscriberIaAppsCategories = {
      val Row(
        timestamp,
        subscriberId,
        appGroup,
        trafficInfo,
        locationId,
        businessEntityId) = row

      SubscriberIaAppsCategories(
        timestamp = timestamp.asInstanceOf[Long],
        subscriberId = subscriberId.asInstanceOf[String],
        appGroup = appGroup.asInstanceOf[String],
        trafficInfo = TrafficInfo.fromRow.fromRow(trafficInfo.asInstanceOf[Row]),
        locationId = locationId.asInstanceOf[String],
        businessEntityId = businessEntityId.asInstanceOf[String])
    }
  }
}
