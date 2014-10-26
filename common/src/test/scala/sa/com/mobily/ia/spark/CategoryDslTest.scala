/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class CategoryDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import CategoryDsl._

  trait WithCategoryText {

    val category1 = "\"0\"|\"0000\"|\"0000\"|\"000000\"|\"All\"|\"1\"|\"1\"|\"\"|\"\""
    val category2 = "\"0\"|\"OTHER\"|\"OTHER\"|\"000000\"|\"All\"|\"0\"|\"0\"|\"\"|\"\""
    val category3 = "\"0\"|\"0000\"|\"0000\"|\"000000\"|\"All\"|\"WrongNumber\"|\"1\"|\"\"|\"\""

    val category = sc.parallelize(List(category1, category2, category3))
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
}
