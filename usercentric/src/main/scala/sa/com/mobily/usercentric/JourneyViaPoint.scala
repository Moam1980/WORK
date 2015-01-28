/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class JourneyViaPoint(
    user: User,
    journeyId: Int,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    cells: Set[(Int, Int)],
    firstEventBeginTime: Long,
    lastEventEndTime: Long,
    numEvents: Long,
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry {

  def fields: Array[String] =
    user.fields ++
      Array(
        journeyId.toString,
        EdmCoreUtils.fmt.print(startTime),
        EdmCoreUtils.fmt.print(endTime),
        geomWkt,
        cells.mkString(EdmCoreUtils.IntraSequenceSeparator),
        EdmCoreUtils.fmt.print(firstEventBeginTime),
        EdmCoreUtils.fmt.print(lastEventEndTime),
        numEvents.toString,
        countryIsoCode)
}

object JourneyViaPoint {

  def apply(slot: SpatioTemporalSlot, journeyId: Int)
      (implicit cellCatalogue: Map[(Int, Int), Cell]): JourneyViaPoint = {
    require(slot.typeEstimate == JourneyViaPointEstimate)
    JourneyViaPoint(
      user = slot.user,
      journeyId = journeyId,
      startTime = slot.startTime,
      endTime = slot.endTime,
      geomWkt = GeomUtils.wkt(slot.geom),
      cells = slot.cells,
      firstEventBeginTime = slot.firstEventBeginTime,
      lastEventEndTime = slot.lastEventEndTime,
      numEvents = slot.numEvents,
      countryIsoCode = slot.countryIsoCode)
  }

  def header: Array[String] =
    User.header ++
      Array("journeyId", "startTime", "endTime", "geomWkt", "cells", "firstEventBeginTime", "lastEventEndTime",
        "numEvents", "countryIsoCode")

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[JourneyViaPoint] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): JourneyViaPoint = {
      val Array(imei, imsi, msisdn, journeyId, startTime, endTime, geomWkt, cells, firstEventBeginTime,
        lastEventEndTime, numEvents, countryIsoCode) = fields

      JourneyViaPoint(
        user = User(imei = imei, imsi = imsi, msisdn = msisdn.toLong),
        journeyId = journeyId.toInt,
        startTime = EdmCoreUtils.fmt.parseDateTime(startTime).getMillis,
        endTime = EdmCoreUtils.fmt.parseDateTime(endTime).getMillis,
        geomWkt = geomWkt,
        cells = Cell.parseCellTuples(cells),
        firstEventBeginTime = EdmCoreUtils.fmt.parseDateTime(firstEventBeginTime).getMillis,
        lastEventEndTime = EdmCoreUtils.fmt.parseDateTime(lastEventEndTime).getMillis,
        numEvents = numEvents.toLong,
        countryIsoCode = countryIsoCode)
    }
  }
}
