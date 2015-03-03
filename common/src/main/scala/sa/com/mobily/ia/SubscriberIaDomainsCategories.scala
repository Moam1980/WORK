/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class SubscriberIaDomainsCategories(
    timestamp: Long,
    subscriberId: String,
    categoryId: String,
    trafficInfo: TrafficInfo,
    locationId: String,
    businessEntityId: String) {

  def fields: Array[String] =
    Array(AggregatedData.Fmt.print(timestamp), subscriberId, categoryId) ++
      trafficInfo.fields ++
      Array(locationId, businessEntityId)
}

object SubscriberIaDomainsCategories extends IaParser {

  val Header: Array[String] =
    Array("date", "subscriberId", "categoryId") ++
      TrafficInfo.Header ++
      Array("locationId", "businessEntityId")

  implicit val fromCsv = new CsvParser[SubscriberIaDomainsCategories] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaDomainsCategories = {
      val Array(dateText, subscriberIdText, categoryIdText, visitCountText, uploadVolumeText, downloadVolumeText,
      totalVolumeText, locationIdText, businessEntityIdText) = fields

      SubscriberIaDomainsCategories(
        timestamp = Fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        categoryId = categoryIdText,
        trafficInfo = TrafficInfo(
          visitCount = visitCountText.toLong,
          uploadVolume = uploadVolumeText.toDouble,
          downloadVolume = downloadVolumeText.toDouble,
          totalVolume = totalVolumeText.toDouble),
        locationId = locationIdText,
        businessEntityId = businessEntityIdText)
    }
  }

  implicit val fromRow = new RowParser[SubscriberIaDomainsCategories] {

    override def fromRow(row: Row): SubscriberIaDomainsCategories = {
      val Row(
        timestamp,
        subscriberId,
        categoryId,
        trafficInfo,
        locationId,
        businessEntityId) = row

      SubscriberIaDomainsCategories(
        timestamp = timestamp.asInstanceOf[Long],
        subscriberId = subscriberId.asInstanceOf[String],
        categoryId = categoryId.asInstanceOf[String],
        trafficInfo = TrafficInfo.fromRow.fromRow(trafficInfo.asInstanceOf[Row]),
        locationId = locationId.asInstanceOf[String],
        businessEntityId = businessEntityId.asInstanceOf[String])
    }
  }
}
