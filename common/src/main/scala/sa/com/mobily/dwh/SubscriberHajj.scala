/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class SubscriberHajj(
    msisdn: Long,
    voiceMostUsedCity: String,
    period: String,
    tenureGrt90: Option[Boolean],
    nationality: String,
    makkahMadinahL3m: Option[Boolean],
    activeLast90d: Option[Boolean],
    tenureActivity: Double,
    flag: String,
    segment: String,
    typeContract: String,
    pack: String,
    category: String,
    rules: String,
    flag2: String)

object SubscriberHajj {

  final val lineCsvParserObject = new OpenCsvParser(separator = ',')

  implicit val fromCsv = new CsvParser[SubscriberHajj] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberHajj = {
      val Array(msisdnText, voiceMostUsedCityText, periodText, tenureGrt90Text, nationalityText, makkahMadinahL3mText,
        activeLast90dText, tenureActivityText, flagText, segmentText, typeContractText, packText, categoryText,
        rulesText, flag2Text) = fields

      SubscriberHajj(
        msisdn = msisdnText.toLong,
        voiceMostUsedCity = voiceMostUsedCityText,
        period = periodText,
        tenureGrt90 = EdmCoreUtils.parseYesNoBoolean(tenureGrt90Text),
        nationality = nationalityText,
        makkahMadinahL3m = EdmCoreUtils.parseYesNoBoolean(makkahMadinahL3mText),
        activeLast90d = EdmCoreUtils.parseYesNoBoolean(activeLast90dText),
        tenureActivity = EdmCoreUtils.parseDouble(tenureActivityText).getOrElse(0D),
        flag = flagText,
        segment = segmentText,
        typeContract = typeContractText,
        pack = packText,
        category = categoryText,
        rules = rulesText,
        flag2 = flag2Text)
    }
  }
}
