/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class Category(
    categoryId: String,
    categoryCd: String,
    categoryFullCs: String,
    parentCategoryId: String,
    bysName: String,
    detlFlag: Int,
    sysFlag: Int,
    imagePath: String,
    categoryDescription: String)

object Category extends IaParser {

  implicit val fromCsv = new CsvParser[Category] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Category = {
      val Array(categoryIdText, categoryCdText, categoryFullCsText, parentCategoryIdText, bysNameText, detlFlagText,
        sysFlagText, imagePathText, categoryDescriptionText) = fields

      Category(
        categoryId = categoryIdText,
        categoryCd = categoryCdText,
        categoryFullCs = categoryFullCsText,
        parentCategoryId = parentCategoryIdText,
        bysName = bysNameText,
        detlFlag = detlFlagText.toInt,
        sysFlag = sysFlagText.toInt,
        imagePath = imagePathText,
        categoryDescription = categoryDescriptionText)
    }
  }
}
