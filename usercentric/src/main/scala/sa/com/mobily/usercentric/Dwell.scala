/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.language.existentials

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.Row

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils
import sa.com.mobily.utils.EdmCoreUtils.FmtDate

case class Dwell(
    user: User,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    cells: Seq[(Int, Int)],
    firstEventBeginTime: Long,
    lastEventEndTime: Long,
    numEvents: Long,
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry {

  def fields: Array[String] =
    user.fields ++
      Array(
        EdmCoreUtils.Fmt.print(startTime),
        EdmCoreUtils.Fmt.print(endTime),
        geomWkt,
        cells.mkString(EdmCoreUtils.IntraSequenceSeparator),
        EdmCoreUtils.Fmt.print(firstEventBeginTime),
        EdmCoreUtils.Fmt.print(lastEventEndTime),
        numEvents.toString,
        countryIsoCode)

  def durationInMinutes: Long = new Duration(startTime, endTime).getStandardMinutes

  def formattedDay: String = FmtDate.print(startTime)
}

object Dwell {

  def apply(slot: SpatioTemporalSlot)(implicit cellCatalogue: Map[(Int, Int), Cell]): Dwell = {
    require(slot.typeEstimate == DwellEstimate)
    Dwell(
      user = slot.user,
      startTime = slot.startTime,
      endTime = slot.endTime,
      geomWkt = GeomUtils.wkt(slot.geom),
      cells = slot.cells.toSeq,
      firstEventBeginTime = slot.firstEventBeginTime,
      lastEventEndTime = slot.lastEventEndTime,
      numEvents = slot.numEvents,
      countryIsoCode = slot.countryIsoCode)
  }

  val Header: Array[String] =
    User.Header ++
      Array("startTime", "endTime", "geomWkt", "cells", "firstEventBeginTime", "lastEventEndTime", "numEvents",
        "countryIsoCode")

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[Dwell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Dwell = {
      val Array(imei, imsi, msisdn, startTime, endTime, geomWkt, cells, firstEventBeginTime, lastEventEndTime,
        numEvents, countryIsoCode) = fields

      Dwell(
        user = User(imei = imei, imsi = imsi, msisdn = msisdn.toLong),
        startTime = EdmCoreUtils.Fmt.parseDateTime(startTime).getMillis,
        endTime = EdmCoreUtils.Fmt.parseDateTime(endTime).getMillis,
        geomWkt = geomWkt,
        cells = Cell.parseCellTuples(cells),
        firstEventBeginTime = EdmCoreUtils.Fmt.parseDateTime(firstEventBeginTime).getMillis,
        lastEventEndTime = EdmCoreUtils.Fmt.parseDateTime(lastEventEndTime).getMillis,
        numEvents = numEvents.toLong,
        countryIsoCode = countryIsoCode)
    }
  }

  implicit val fromRow = new RowParser[Dwell] {

    override def fromRow(row: Row): Dwell = {
      val Seq(Seq(imei, imsi, msisdn), startTime, endTime, geomWkt, cells, firstEventBeginTime, lastEventEndTime,
        numEvents, countryIsoCode) = row.toSeq

      Dwell(
        user =
          User(imei = imei.asInstanceOf[String], imsi = imsi.asInstanceOf[String], msisdn = msisdn.asInstanceOf[Long]),
        startTime = startTime.asInstanceOf[Long],
        endTime = endTime.asInstanceOf[Long],
        geomWkt = geomWkt.asInstanceOf[String],
        cells = cells.asInstanceOf[Seq[Seq[Int]]].map { case Seq(first: Int, second: Int) => (first, second) },
        firstEventBeginTime = firstEventBeginTime.asInstanceOf[Long],
        lastEventEndTime = lastEventEndTime.asInstanceOf[Long],
        numEvents = numEvents.asInstanceOf[Long],
        countryIsoCode = countryIsoCode.asInstanceOf[String])
    }
  }
}
