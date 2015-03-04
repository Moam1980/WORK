/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

class UserModelStatsViewTest extends FlatSpec with ShouldMatchers {

  trait WithUserModelStatsView {

    val userModelStatsView1 = UserModelStatsView(TotalNumberOfDwells, "01/01/2010", 1.0)
    val userModelStatsView2 = UserModelStatsView(TotalNumberOfUsersByDwells, "02/01/2010", 2.0)
    val userModelStatsView3 = UserModelStatsView(MeanDwellsByUser, "03/01/2010", 3.0)
    val userModelStatsView4 = UserModelStatsView(StdDeviationDwellsByUser, "04/01/2010", 4.0)
    val userModelStatsView5 = UserModelStatsView(MeanDwellsArea, "05/01/2010", 5.0)
    val userModelStatsView6 = UserModelStatsView(StdDeviationDwellsArea, "06/01/2010", 6.0)
    val userModelStatsView7 = UserModelStatsView(MeanDwellsDuration, "07/01/2010", 7.0)
    val userModelStatsView8 = UserModelStatsView(StdDeviationDwellsDuration, "08/01/2010", 8.0)
  }

  "UserModelStatsView" should "get the stats view as csv fields" in new WithUserModelStatsView {
    userModelStatsView1.toCsvFields should be("TotalNumberOfDwells|01/01/2010|1.0")
    userModelStatsView2.toCsvFields should be("TotalNumberOfUsersByDwells|02/01/2010|2.0")
    userModelStatsView3.toCsvFields should be("MeanDwellsByUser|03/01/2010|3.0")
    userModelStatsView4.toCsvFields should be("StdDeviationDwellsByUser|04/01/2010|4.0")
    userModelStatsView5.toCsvFields should be("MeanDwellsArea|05/01/2010|5.0")
    userModelStatsView6.toCsvFields should be("StdDeviationDwellsArea|06/01/2010|6.0")
    userModelStatsView7.toCsvFields should be("MeanDwellsDuration|07/01/2010|7.0")
    userModelStatsView8.toCsvFields should be("StdDeviationDwellsDuration|08/01/2010|8.0")
  }
}