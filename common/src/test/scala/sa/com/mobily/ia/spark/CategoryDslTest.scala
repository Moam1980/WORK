/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.Category
import sa.com.mobily.utils.LocalSparkSqlContext

class CategoryDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import CategoryDsl._

  trait WithCategoryText {

    val category1 = "\"0\"|\"0000\"|\"0000\"|\"000000\"|\"All\"|\"1\"|\"1\"|\"\"|\"\""
    val category2 = "\"0\"|\"OTHER\"|\"OTHER\"|\"000000\"|\"All\"|\"0\"|\"0\"|\"\"|\"\""
    val category3 = "\"0\"|\"0000\"|\"0000\"|\"000000\"|\"All\"|\"WrongNumber\"|\"1\"|\"\"|\"\""

    val category = sc.parallelize(List(category1, category2, category3))
  }

  trait WithCategoryRows {

    val row = Row("0", "0000", "0000", "000000", "All", 1, 1, "", "")
    val row2 = Row("0", "OTHER", "OTHER", "000000", "All", 0, 0, "", "")
    val wrongRow = Row("0", "0000", "0000", "000000", "All", "WrongNumber", 0, "", "")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithCategory {
    
    val category1 =
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
    val category2 =
      Category(
        categoryId = "0",
        categoryCd = "OTHER",
        categoryFullCs = "OTHER",
        parentCategoryId = "000000",
        bysName = "All",
        detlFlag = 0,
        sysFlag = 0,
        imagePath = "",
        categoryDescription = "")
    val category3 =
      Category(
        categoryId = "categoryId_3",
        categoryCd = "categoryCd_3",
        categoryFullCs = "categoryFullCs_3",
        parentCategoryId = "parentCategoryId_3",
        bysName = "bysName_3",
        detlFlag = 1,
        sysFlag = 1,
        imagePath = "imagePath_3",
        categoryDescription = "categoryDescription_3")

    val categories = sc.parallelize(List(category1, category2, category3))
  }

  "CategoryDsl" should "get correctly parsed data" in new WithCategoryText {
    category.toCategory.count should be (2)
  }

  it should "get errors when parsing data" in new WithCategoryText {
    category.toCategoryErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithCategoryText {
    category.toParsedCategory.count should be (3)
  }

  it should "get correctly parsed rows" in new WithCategoryRows {
    rows.toCategory.count should be (2)
  }

  it should "save in parquet" in new WithCategory {
    val path = File.makeTemp().name
    categories.saveAsParquetFile(path)
    sqc.parquetFile(path).toCategory.collect should be (categories.collect)
    File(path).deleteRecursively
  }
}
