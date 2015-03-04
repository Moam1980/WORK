/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.utils.EdmCoreUtils

sealed case class UserModelStatsMetricType(identifier: String)

object TotalNumberOfDwells extends UserModelStatsMetricType(identifier = "TotalNumberOfDwells")
object TotalNumberOfUsersByDwells extends UserModelStatsMetricType(identifier = "TotalNumberOfUsersByDwells")
object MeanDwellsByUser extends UserModelStatsMetricType(identifier = "MeanDwellsByUser")
object StdDeviationDwellsByUser extends UserModelStatsMetricType(identifier = "StdDeviationDwellsByUser")
object MeanDwellsArea extends UserModelStatsMetricType(identifier = "MeanDwellsArea")
object StdDeviationDwellsArea extends UserModelStatsMetricType(identifier = "StdDeviationDwellsArea")
object MeanDwellsDuration extends UserModelStatsMetricType(identifier = "MeanDwellsDuration")
object StdDeviationDwellsDuration extends UserModelStatsMetricType(identifier = "StdDeviationDwellsDuration")

case class UserModelStatsView(metricType: UserModelStatsMetricType, date: String, count: Double) {

  def toCsvFields: String = Array(metricType.identifier, date, count.toString).mkString(EdmCoreUtils.Separator)
}
