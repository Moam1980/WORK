/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class SubscriberIaVolume(
    subscriberId: String,
    volumeBytes: Double)

object SubscriberIaVolume extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIaVolume] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaVolume = {
      val Array(subscriberIdText, totalVolumeText) = fields

      SubscriberIaVolume(
        subscriberId = subscriberIdText,
        volumeBytes = totalVolumeText.toDouble)
    }
  }
}
