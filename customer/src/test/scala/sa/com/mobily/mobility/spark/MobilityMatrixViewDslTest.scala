/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import org.scalatest._

import sa.com.mobily.mobility.MobilityMatrixView
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class MobilityMatrixViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import MobilityMatrixViewDsl._

  trait WithMobilityMatrixViews {

    val user1 = User("", "4200301", 96601)
    val user2 = User("", "4200302", 96602)
    val user3 = User("", "4200303", 96603)

    val view1_1 = MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 1800, 0.3, 2,
      Set(user1, user2))
    val view1_2 = MobilityMatrixView("Tue-Wed 12:00:00", "Tue-Wed 13:00:00", "l3", "l1", 110, 0.1, 2,
      Set(user1, user3))

    val view2_1 = MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 200, 0.6, 2, Set(user3))
    val view2_2 = MobilityMatrixView("Tue-Wed 12:00:00", "Tue-Wed 13:00:00", "l3", "l1", 900, 0.4, 2, Set(user3))

    val aggView1 =
      MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 2000, 0.8999999999999999, 2,
        Set(user1, user2, user3))
    val aggView2 =
      MobilityMatrixView("Tue-Wed 12:00:00", "Tue-Wed 13:00:00", "l3", "l1", 1010, 0.5, 2, Set(user1, user3))
    val aggViews = List(aggView1, aggView2)

    val diffView1 =
      MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 1600, 0.3, 2,
        Set(user1, user2, user3))
    val diffView2 =
      MobilityMatrixView("Tue-Wed 12:00:00", "Tue-Wed 13:00:00", "l3", "l1", 790, 0.30000000000000004, 2, Set(user1))
    val diffViews = List(diffView1, diffView2)

    val views = sc.parallelize(Array(view1_1, view1_2))
    val viewsOther = sc.parallelize(Array(view2_1, view2_2))
  }

  "MobilityMatrixViewDsl" should "return aggregated items" in new WithMobilityMatrixViews {
    views.agg(viewsOther).collect should contain theSameElementsAs(aggViews)
  }

  it should "return difference items" in new WithMobilityMatrixViews {
    views.diff(viewsOther).collect should contain theSameElementsAs(diffViews)
  }
}
