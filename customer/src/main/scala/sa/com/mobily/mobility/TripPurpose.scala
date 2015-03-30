/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

sealed case class TripPurpose(identifier: String)

object HomeToWork extends TripPurpose(identifier = "Home to Work")
object HomeToOther extends TripPurpose(identifier = "Home to Other")
object NonHomeBased extends TripPurpose(identifier = "Non Home based")
