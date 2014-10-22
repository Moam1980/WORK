/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class CategoryTest extends FlatSpec with ShouldMatchers {

  import Category._

  trait WithCategory {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val categoryLine = "\"0\"|\"0000\"|\"0000\"|\"000000\"|\"All\"|\"1\"|\"1\"|\"\"|\"\""
    val fields = Array("0", "0000", "0000", "000000", "All", "1", "1", "", "")

    val category = Category("0", "0000", "0000", "000000", "All", 1, 1, "", "")
  }

  "Category" should "be built from CSV" in new WithCategory {
    CsvParser.fromLine(categoryLine).value.get should be (category)
  }

  it should "be discarded when the CSV format is wrong" in new WithCategory {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }
}
