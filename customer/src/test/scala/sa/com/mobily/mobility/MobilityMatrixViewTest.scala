/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import org.joda.time.Duration
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

class MobilityMatrixViewTest extends FlatSpec with ShouldMatchers {

  trait WithIntervals {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val endDate = startDate.plusHours(3)
    val intervals = EdmCoreUtils.intervals(startDate, endDate, 60)
  }

  trait WithUsers {

    val user1 = User("", "4200301", 96601)
    val user2 = User("", "4200302", 96602)
    val user3 = User("", "4200303", 96603)
  }

  trait WithMobilityMatrixItem extends WithIntervals with WithUsers {

    val mobilityMatrixItem = MobilityMatrixItem(intervals(0), intervals(1), "l1", "l2", new Duration(1800000L), 4,
      user1, 0.4, 0.6)
  }

  trait WithMobilityMatrixViews extends WithMobilityMatrixItem {

    val mobilityMatrixView =
      MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 1800, 1, 2, Set(user1, user2))
    val mobilityMatrixViewFromItem =
      MobilityMatrixView("00:00:00", "01:00:00", "l1", "l2", 900, 0.5, 28, Set(user1))

    val view1 = MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 1800, 0.3, 2, Set(user1, user2))
    val view2 = MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 900, 0.4, 2, Set(user3))
    val aggView =
      MobilityMatrixView("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", 2700, 0.7, 2, Set(user1, user2, user3))
  }

  "MobilityMatrixView" should "return fields" in new WithMobilityMatrixViews {
    mobilityMatrixView.fields should be (Array("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2", "30", "0.5", "2"))
  }

  it should "have the proper header" in {
    MobilityMatrixView.Header should be (Array("StartIntervalInitTime", "EndIntervalInitTime", "StartLocation",
      "EndLocation", "AvgJourneyDurationInMinutes", "AvgWeight", "NumDistinctUsers"))
  }

  it should "have same number of fields and header" in new WithMobilityMatrixViews {
    mobilityMatrixView.fields.size should be (MobilityMatrixView.Header.size)
  }

  it should "compute the average journey duration in minutes" in new WithMobilityMatrixViews {
    mobilityMatrixView.avgJourneyDurationInMinutes should be (30)
  }

  it should "compute the average weight" in new WithMobilityMatrixViews {
    mobilityMatrixView.avgWeight should be (0.5)
  }

  it should "compute the number of distinct users seen" in new WithMobilityMatrixViews {
    mobilityMatrixView.numDistinctUsers should be (2)
  }

  it should "get the fields to use as key" in new WithMobilityMatrixViews {
    mobilityMatrixView.key should be (("Tue-Wed 13:00:00", "Tue-Wed 14:00:00", "l1", "l2"))
  }

  it should "build from MobilityMatrixItem with relevant time bin and number of periods within one week" in
    new WithMobilityMatrixViews {
      MobilityMatrixView(
        mobilityMatrixItem,
        (d) => MobilityMatrixView.TimeFmt.print(d),
        (d1, d2) => EdmCoreUtils.DaysInWeek) should be (mobilityMatrixViewFromItem)
    }

  it should "aggregate two MobilityMatrixItem" in new WithMobilityMatrixViews {
    MobilityMatrixView.aggregate(view1, view2) should be (aggView)
  }

  it should "compute ADA time bins for normal working days pattern" in {
    MobilityMatrixView.adaTimeBin(EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")) should be ("Sun-Wed 00:00:00")
  }

  it should "compute ADA time bins for non-normal working (or non-working) days pattern" in {
    MobilityMatrixView.adaTimeBin(EdmCoreUtils.Fmt.parseDateTime("2014/11/06 07:00:00")) should be ("Thu 07:00:00")
  }

  it should "compute ADA number of periods for normal working days pattern" in {
    MobilityMatrixView.adaNumDaysWithinOneWeek(
      EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00"),
      EdmCoreUtils.Fmt.parseDateTime("2014/11/02 01:00:00")) should be (4)
  }

  it should "compute ADA number of periods for non-normal working (or non-working) days pattern" in {
    MobilityMatrixView.adaNumDaysWithinOneWeek(
      EdmCoreUtils.Fmt.parseDateTime("2014/11/05 23:00:00"),
      EdmCoreUtils.Fmt.parseDateTime("2014/11/06 00:00:00")) should be (1)
  }
}
