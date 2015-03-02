/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.utils.EdmCoreUtils

case class SubscriberIaSearches(
    timestamp: Long,
    subscriberId: String,
    keyword: String,
    domainName: String,
    visitDate: Long,
    engineName: String,
    category: String,
    locationId: String,
    businessEntityId: String) {

  def fields: Array[String] =
    Array(AggregatedData.Fmt.print(timestamp), subscriberId, keyword, domainName, AggregatedData.Fmt.print(visitDate),
      engineName, category, locationId, businessEntityId)
}

object SubscriberIaSearches extends IaParser {

  val Header: Array[String] =
    Array("date", "subscriberId", "keyword", "domainName", "visitDate", "engineName", "category", "locationId",
      "businessEntityId")

  implicit val fromCsv = new CsvParser[SubscriberIaSearches] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaSearches = {
      val Array(dateText, subscriberIdText, keywordText, domainNameText, visitDateText, engineNameText, categoryText,
        locationIdText, businessEntityIdText) = fields

      SubscriberIaSearches(
        timestamp = Fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        keyword = keywordText,
        domainName = domainNameText,
        visitDate = visitDateText.toLong * EdmCoreUtils.MillisInSecond,
        engineName = engineNameText,
        category = categoryText,
        locationId = locationIdText,
        businessEntityId = businessEntityIdText)
    }
  }

  implicit val fromRow = new RowParser[SubscriberIaSearches] {

    override def fromRow(row: Row): SubscriberIaSearches = {
      val Row(
        timestamp,
        subscriberId,
        keyword,
        domainName,
        visitDate,
        engineName,
        category,
        locationId,
        businessEntityId) = row

      SubscriberIaSearches(
        timestamp = timestamp.asInstanceOf[Long],
        subscriberId = subscriberId.asInstanceOf[String],
        keyword = keyword.asInstanceOf[String],
        domainName = domainName.asInstanceOf[String],
        visitDate = visitDate.asInstanceOf[Long],
        engineName = engineName.asInstanceOf[String],
        category = category.asInstanceOf[String],
        locationId = locationId.asInstanceOf[String],
        businessEntityId = businessEntityId.asInstanceOf[String])
    }
  }
}
