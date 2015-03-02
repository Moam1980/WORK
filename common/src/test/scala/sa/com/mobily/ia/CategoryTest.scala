/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class CategoryTest extends FlatSpec with ShouldMatchers {

  import Category._

  trait WithCategory {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val categoryLine = "\"0\"|\"0000\"|\"0000\"|\"000000\"|\"All\"|\"1\"|\"1\"|\"\"|\"\""
    val fields = Array("0", "0000", "0000", "000000", "All", "1", "1", "", "")

    val categoryFields = Array("0", "0000", "0000", "000000", "All", "1", "1", "", "")
    val categoryHeader =
      Array("categoryId", "categoryCd", "categoryFullCs", "parentCategoryId", "bysName", "detlFlag", "sysFlag",
        "imagePath", "categoryDescription")

    val row = Row("0", "0000", "0000", "000000", "All", 1, 1, "", "")
    val wrongRow = Row("0", "0000", "0000", "000000", "All", "NotANumber", 1, "", "")

    val category =
      Category(
        categoryId = "0",
        categoryCd = "0000",
        categoryFullCs = "0000",
        parentCategoryId = "000000",
        bysName = "All",
        detlFlag = 1,
        sysFlag = 1,
        imagePath = "",
        categoryDescription = "")
  }

  "Category" should "return correct header" in new WithCategory {
    Category.Header should be (categoryHeader)
  }

  it should "return correct fields" in new WithCategory {
    category.fields should be (categoryFields)
  }

  it should "have same number of elements fields and header" in new WithCategory {
    category.fields.length should be (Category.Header.length)
  }

  it should "be built from CSV" in new WithCategory {
    CsvParser.fromLine(categoryLine).value.get should be (category)
  }

  it should "be discarded when the CSV format is wrong" in new WithCategory {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "be built from Row" in new WithCategory {
    fromRow.fromRow(row) should be (category)
  }

  it should "be discarded when row is wrong" in new WithCategory {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
