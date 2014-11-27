/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.roaming.CountryCode

case class Dwell(
    user: Long,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    orderedCells: List[(Int, Int)],
    firstEventBeginTime: Long,
    lastEventEndTime: Long,
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry with CellSequence
