/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class SubscriberIaSearches(
    timestamp: Long,
    subscriberId: String,
    keyword: String,
    domainName: String,
    visitDate: Long,
    engineName: String,
    category: String,
    locationId: String,
    businessEntityId: String)

object SubscriberIaSearches extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIaSearches] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaSearches = {
      val Array(dateText, subscriberIdText, keywordText, domainNameText, visitDateText, engineNameText, categoryText,
        locationIdText, businessEntityIdText) = fields

      SubscriberIaSearches(
        timestamp = fmt.parseDateTime(dateText).getMillis,
        subscriberId = subscriberIdText,
        keyword = keywordText,
        domainName = domainNameText,
        visitDate = visitDateText.toLong,
        engineName = engineNameText,
        category = categoryText,
        locationId = locationIdText,
        businessEntityId = businessEntityIdText)
    }
  }
}
