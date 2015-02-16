/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell

case class Footfall(users: Set[User], numDwells: Int, avgPrecision: Double) {

  lazy val numDistinctUsers = users.size

  def fields: Array[String] = Array(numDistinctUsers.toString, numDwells.toString, avgPrecision.toString)
}

object Footfall {

  def apply(dwell: Dwell): Footfall =
    Footfall(users = Set(dwell.user), numDwells = 1, avgPrecision = dwell.geom.getArea)

  def aggregate(ff1: Footfall, ff2: Footfall): Footfall =
    Footfall(
      users = ff1.users ++ ff2.users,
      numDwells = ff1.numDwells + ff2.numDwells,
      avgPrecision =
        (ff1.avgPrecision * ff1.numDwells + ff2.avgPrecision * ff2.numDwells) / (ff1.numDwells + ff2.numDwells))

  def header: Array[String] = Array("footfall", "numDwells", "avgPrecision")
}
