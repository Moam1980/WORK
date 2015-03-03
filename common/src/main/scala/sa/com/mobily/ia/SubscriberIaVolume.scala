/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class SubscriberIaVolume(
    subscriberId: String,
    volumeBytes: Double) {

  def fields: Array[String] = Array(subscriberId, volumeBytes.toString)
}

object SubscriberIaVolume extends IaParser {

  val Header: Array[String] = Array("subscriberId", "volumeBytes")

  implicit val fromCsv = new CsvParser[SubscriberIaVolume] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIaVolume = {
      val Array(subscriberIdText, totalVolumeText) = fields

      SubscriberIaVolume(
        subscriberId = subscriberIdText,
        volumeBytes = totalVolumeText.toDouble)
    }
  }

  implicit val fromRow = new RowParser[SubscriberIaVolume] {

    override def fromRow(row: Row): SubscriberIaVolume = {
      val Row(subscriberId, volumeBytes) = row

      SubscriberIaVolume(
        subscriberId = subscriberId.asInstanceOf[String],
        volumeBytes = volumeBytes.asInstanceOf[Double])
    }
  }
}
