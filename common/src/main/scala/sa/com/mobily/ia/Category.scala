/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class Category(
    categoryId: String,
    categoryCd: String,
    categoryFullCs: String,
    parentCategoryId: String,
    bysName: String,
    detlFlag: Int,
    sysFlag: Int,
    imagePath: String,
    categoryDescription: String) {

  def fields: Array[String] =
    Array(categoryId,
      categoryCd,
      categoryFullCs,
      parentCategoryId,
      bysName,
      detlFlag.toString,
      sysFlag.toString,
      imagePath,
      categoryDescription)
}

object Category extends IaParser {

  val Header: Array[String] =
    Array("categoryId", "categoryCd", "categoryFullCs", "parentCategoryId", "bysName", "detlFlag", "sysFlag",
      "imagePath", "categoryDescription")

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

  implicit val fromRow = new RowParser[Category] {

    override def fromRow(row: Row): Category = {
      val Row(
        categoryId,
        categoryCd,
        categoryFullCs,
        parentCategoryId,
        bysName,
        detlFlag,
        sysFlag,
        imagePath,
        categoryDescription) = row

      Category(
        categoryId = categoryId.asInstanceOf[String],
        categoryCd = categoryCd.asInstanceOf[String],
        categoryFullCs = categoryFullCs.asInstanceOf[String],
        parentCategoryId = parentCategoryId.asInstanceOf[String],
        bysName = bysName.asInstanceOf[String],
        detlFlag = detlFlag.asInstanceOf[Int],
        sysFlag = sysFlag.asInstanceOf[Int],
        imagePath = imagePath.asInstanceOf[String],
        categoryDescription = categoryDescription.asInstanceOf[String])
    }
  }
}
