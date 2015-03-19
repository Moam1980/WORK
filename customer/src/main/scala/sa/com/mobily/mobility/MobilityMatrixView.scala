/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import com.github.nscala_time.time.Imports._

import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class MobilityMatrixView(
    startIntervalInitTime: String,
    endIntervalInitTime: String,
    startLocation: String,
    endLocation: String,
    sumWeightedJourneyDurationInSeconds: Double,
    sumWeight: Double,
    numPeriods: Int,
    users: Set[User]) {

  lazy val avgJourneyDurationInMinutes =
    (sumWeightedJourneyDurationInSeconds / (sumWeight * EdmCoreUtils.SecondsInMinute)).round
  lazy val avgWeight = sumWeight / numPeriods
  lazy val numDistinctUsers = users.size

  def key: (String, String, String, String) = (startIntervalInitTime, endIntervalInitTime, startLocation, endLocation)

  def fields: Array[String] =
    Array(
      startIntervalInitTime,
      endIntervalInitTime,
      startLocation,
      endLocation,
      avgJourneyDurationInMinutes.toString,
      avgWeight.toString,
      numDistinctUsers.toString)
}

object MobilityMatrixView {

  val DefaultMinUsersPerJourney = 5
  val WeekDayDateFormatter = "E HH:mm:ss"
  val WeekDayFmt = DateTimeFormat.forPattern(WeekDayDateFormatter)
  val TimeDateFormatter = "HH:mm:ss"
  val TimeFmt = DateTimeFormat.forPattern(TimeDateFormatter)
  private val SundayIndex = 7
  private val MondayIndex = 1
  private val TuesdayIndex = 2
  private val WednesdayIndex = 3
  private val AdaLabourPatternDays = List(SundayIndex, MondayIndex, TuesdayIndex, WednesdayIndex)
  private val AdaLabourPatternDaysPrint = "Sun-Wed"

  val Header: Array[String] =
    Array(
      "StartIntervalInitTime",
      "EndIntervalInitTime",
      "StartLocation",
      "EndLocation",
      "AvgJourneyDurationInMinutes",
      "AvgWeight",
      "NumDistinctUsers")

  def apply(
      item: MobilityMatrixItem,
      timeBin: (DateTime) => String,
      numDaysWithinOneWeek: (DateTime, DateTime) => Int): MobilityMatrixView =
    MobilityMatrixView(
      startIntervalInitTime = timeBin(item.startInterval.start),
      endIntervalInitTime = timeBin(item.endInterval.start),
      startLocation = item.startLocation,
      endLocation = item.endLocation,
      sumWeightedJourneyDurationInSeconds = item.journeyDuration.getStandardSeconds * item.weight,
      sumWeight = item.weight,
      numPeriods = numDaysWithinOneWeek(item.startInterval.start, item.endInterval.start) * item.numWeeks,
      users = Set(item.user))

  def aggregate(viewItem1: MobilityMatrixView, viewItem2: MobilityMatrixView): MobilityMatrixView =
    MobilityMatrixView(
      startIntervalInitTime = viewItem1.startIntervalInitTime,
      endIntervalInitTime = viewItem1.endIntervalInitTime,
      startLocation = viewItem1.startLocation,
      endLocation = viewItem1.endLocation,
      sumWeightedJourneyDurationInSeconds =
        viewItem1.sumWeightedJourneyDurationInSeconds + viewItem2.sumWeightedJourneyDurationInSeconds,
      sumWeight = viewItem1.sumWeight + viewItem2.sumWeight,
      numPeriods = viewItem1.numPeriods,
      users = viewItem1.users ++ viewItem2.users)

  def adaTimeBin(date: DateTime): String =
    if (AdaLabourPatternDays.contains(date.getDayOfWeek)) AdaLabourPatternDaysPrint + " " + TimeFmt.print(date)
    else WeekDayFmt.print(date)

  def adaNumDaysWithinOneWeek(start: DateTime, end: DateTime): Int =
    if (AdaLabourPatternDays.contains(start.getDayOfWeek) && AdaLabourPatternDays.contains(end.getDayOfWeek))
      AdaLabourPatternDays.size
    else 1
}
