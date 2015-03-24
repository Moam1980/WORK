/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

class MobilityMatrixItemParquetTest extends FlatSpec with ShouldMatchers {

  import MobilityMatrixItemParquet._

  trait WithIntervals {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val endDate = startDate.plusHours(3)
    val intervals = EdmCoreUtils.intervals(startDate, endDate, 60)
  }

  trait WithMobilityMatrixItemParquets extends WithIntervals {

    val row =
      Row(intervals(0).getStart.getZone.getID, intervals(0).getStartMillis, intervals(0).getEndMillis,
        intervals(1).getStartMillis, intervals(1).getEndMillis, "loc1", "loc2", 1800000L, 4, Row("", "4200301", 0L),
        0.4, 0.6)
    val wrongRow =
      Row("", intervals(1).getStartMillis, intervals(1).getEndMillis, "loc1", "loc2", new Duration(1800000L), 4,
        Row("", "4200301", 0), 0.4, 0.6)

    val item =
      MobilityMatrixItemParquet(
        intervals(0).getStart.getZone.getID,
        intervals(0).getStartMillis,
        intervals(0).getEndMillis,
        intervals(1).getStartMillis,
        intervals(1).getEndMillis,
        "loc1", "loc2", 1800000L, 4, User("", "4200301", 0), 0.4, 0.6)

    val mobilityMatrixItem =
      MobilityMatrixItem(intervals(0), intervals(1), "loc1", "loc2", new Duration(1800000L), 4,
        User("", "4200301", 0), 0.4, 0.6)
  }

  "MobilityMatrixItemParquet" should "be built from Row" in new WithMobilityMatrixItemParquets {
    fromRow.fromRow(row) should be (item)
  }

  it should "be discarded when row is wrong" in new WithMobilityMatrixItemParquets {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "apply from mobility matrix item" in new WithMobilityMatrixItemParquets {
    MobilityMatrixItemParquet(mobilityMatrixItem) should be (item)
  }
}
