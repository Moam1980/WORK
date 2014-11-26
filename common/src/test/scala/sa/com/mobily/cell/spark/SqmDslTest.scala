/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SqmDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SqmDsl._

  trait WithSqmCellsText {

    val sqmCell1 = "\"eNB_446539_0\",\"4465390\",\"6539\",\"4465390\",\"4465390\",\"YB6539_P3_LTE\",\"57\"," +
      "\"24.0056\",\"38.1849\",\"HUAWEI\",\"4G_TDD\",\"MACRO\",\"25\",\"0\"," +
      "\"M1P1 and M1P2 and M1P3 and M1P4 and M1P5\",\"NORTH\",\"SECTOR\",15.2,-128,\"2600\""
    val sqmCell2 = "\"eNB_446539_0\",\"4465390\",\"6539\",\"4465390\",\"4465390\",\"YB6539_P3_LTE\",\"57\"," +
      "\"24.0056\",\"38.1849\",\"HUAWEI\",\"4G_TDD\",\"MACRO\",\"25\",\"0\"," +
      "\"M1P1 and M1P2 and M1P3 and M1P4 and M1P5\",\"NORTH\",\"SECTOR\",15.2,-128,\"2600\""
    val sqmCell3 = "\"eNB_446539_0\",\"4465390\",\"6539\",\"4465390\",\"4465390\",\"YB6539_P3_LTE\",\"57\"," +
      "\"24.0056\",\"38.1849\",\"HUAWEI\",\"WRONG\",\"MACRO\",\"25\",\"0\"," +
      "\"M1P1 and M1P2 and M1P3 and M1P4 and M1P5\",\"NORTH\",\"SECTOR\",15.2,-128,\"2600\""

    val sqmCells = sc.parallelize(List(sqmCell1, sqmCell2, sqmCell3))
  }

  "SqmContext" should "get correctly parsed Sqm cells" in new WithSqmCellsText {
    sqmCells.toSqmCell.count should be (2)
  }

  it should "get errors when parsing Sqm cells" in new WithSqmCellsText {
    sqmCells.toSqmCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed Sqm cells" in new WithSqmCellsText {
    sqmCells.toParsedSqmCell.count should be (3)
  }
}
