/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec
import scala.language.existentials

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.apache.spark.sql._

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class Journey(
    user: User,
    id: Int,
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
        id.toString,
        EdmCoreUtils.Fmt.print(startTime),
        EdmCoreUtils.Fmt.print(endTime),
        geomWkt,
        cells.mkString(EdmCoreUtils.IntraSequenceSeparator),
        EdmCoreUtils.Fmt.print(firstEventBeginTime),
        EdmCoreUtils.Fmt.print(lastEventEndTime),
        numEvents.toString,
        countryIsoCode)
}

object Journey {

  val ZeroSpeed = Some(0.0)
  val AvgWalkingSpeed = 1.38889 // 5km/h

  def apply(
      orig: SpatioTemporalSlot,
      dest: SpatioTemporalSlot,
      id: Integer,
      viaPoints: List[JourneyViaPoint])
      (implicit cellCatalogue: Map[(Int, Int), Cell]): Journey = {
    require(orig.typeEstimate == DwellEstimate && dest.typeEstimate == DwellEstimate)
    Journey(
      user = orig.user,
      id = id,
      startTime = orig.endTime,
      endTime = dest.startTime,
      geomWkt = journeyGeometry(orig, dest, viaPoints),
      cells = viaPoints.flatMap(_.cells).toSet.toSeq,
      firstEventBeginTime = viaPoints.headOption.map(_.firstEventBeginTime).getOrElse(orig.endTime),
      lastEventEndTime = viaPoints.lastOption.map(_.lastEventEndTime).getOrElse(dest.startTime),
      numEvents = viaPoints.map(_.numEvents).sum,
      countryIsoCode = orig.countryIsoCode)
  }

  def journeyGeometry(
      orig: SpatioTemporalSlot,
      dest: SpatioTemporalSlot,
      viaPoints: List[JourneyViaPoint])
      (implicit cellCatalogue: Map[(Int, Int), Cell]): String =
    GeomUtils.wkt(orig.geom.getFactory.createLineString(
      Array(orig.geom.getCentroid.getCoordinate) ++
        viaPoints.map(_.geom.getCentroid.getCoordinate) :+
        dest.geom.getCentroid.getCoordinate))

  def header: Array[String] =
    User.header ++
      Array("id", "startTime", "endTime", "geomWkt", "cells", "firstEventBeginTime", "lastEventEndTime",
        "numEvents", "countryIsoCode")

  // scalastyle:off method.length
  def computeMinSpeed(events: List[Event], cells: Map[(Int, Int), Cell]): List[Event] = {
    val geom = Event.geom(cells) _
    val geomFactory = cells.headOption.map(cellTuple =>
      GeomUtils.geomFactory(cellTuple._2.coverageGeom.getSRID, cellTuple._2.coverageGeom.getPrecisionModel)).getOrElse(
        GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid))

    @tailrec
    def fillMinSpeed(
        events: List[Event],
        result: List[Event] = List()): List[Event] = {
      val initPoint =
        GeomUtils.parseWkt(events.head.minSpeedPointWkt.get, geomFactory.getSRID, geomFactory.getPrecisionModel)
      events match {
        case first :: Nil => result :+ first.copy(outSpeed = ZeroSpeed)
        case first :: second :: Nil =>
          val initPointInSecond =
            GeomUtils.ensureNearestPointInGeom(
              geomFactory.createPoint(DistanceOp.nearestPoints(initPoint, geom(second)).last),
              geom(second))
          val speed = DistanceOp.distance(initPoint, initPointInSecond) / secondsInBetween(first, second)
          val newSecond =
            second.copy(
              inSpeed = Some(speed),
              outSpeed = ZeroSpeed,
              minSpeedPointWkt = Some(GeomUtils.wkt(initPointInSecond)))
          result :+ first.copy(outSpeed = Some(speed)) :+ newSecond
        case first :: second :: tail if geom(second).intersects(initPoint) =>
          fillMinSpeed(
            second.copy(inSpeed = ZeroSpeed, minSpeedPointWkt = Some(GeomUtils.wkt(initPoint))) :: tail,
            result :+ first.copy(outSpeed = ZeroSpeed))
        case first :: second :: third :: tail =>
          val closestInSecondToInit =
            GeomUtils.ensureNearestPointInGeom(
              geomFactory.createPoint(DistanceOp.nearestPoints(initPoint, geom(second)).last),
              geom(second))
          val initPointInSecond =
            nextInitPoint(
              closestInSecondToInit = closestInSecondToInit,
              second = geom(second),
              third = geom(third),
              geomFactory = geomFactory)
          val speed = initPoint.distance(initPointInSecond) / secondsInBetween(first, second)
          val newSecond = second.copy(inSpeed = Some(speed), minSpeedPointWkt = Some(GeomUtils.wkt(initPointInSecond)))
          fillMinSpeed(
            newSecond :: third :: tail,
            result :+ first.copy(outSpeed = Some(speed)))
      }
    }

    if (events.isEmpty) List()
    else {
      val startingPoint = geom(events.head).getCentroid // TODO: Might need to refine and get a more appropriate point
      fillMinSpeed(
        events.head.copy(inSpeed = ZeroSpeed, minSpeedPointWkt = Some(GeomUtils.wkt(startingPoint))) :: events.tail)
    }
  }
  // scalastyle:on method.length

  def secondsInBetween(firstEvent: Event, secondEvent: Event): Double = {
    val difference: Double = secondEvent.beginTime - firstEvent.beginTime
    if (difference == 0) 1d / EdmCoreUtils.MillisInSecond
    else difference / EdmCoreUtils.MillisInSecond
  }

  def nextInitPoint(
      closestInSecondToInit: Point,
      second: Geometry,
      third: Geometry,
      geomFactory: GeometryFactory): Point = {
    val closestInThird = geomFactory.createPoint(DistanceOp.nearestPoints(closestInSecondToInit, third).last)
    val line =
      geomFactory.createLineString(Array(closestInSecondToInit.getCoordinate, closestInThird.getCoordinate))
    val throughSecondCandidate = if (line.isValid) line.intersection(second) else closestInSecondToInit
    val geomThroughSecond = if (!throughSecondCandidate.isEmpty) throughSecondCandidate else closestInSecondToInit
    val closestInLineThroughSecondToThird = DistanceOp.nearestPoints(geomThroughSecond, third).head
    val midPointThroughSecond = geomFactory.createPoint(
      LineSegment.midPoint(closestInSecondToInit.getCoordinate, closestInLineThroughSecondToThird))
    GeomUtils.ensureNearestPointInGeom(
      geomFactory.createPoint(DistanceOp.nearestPoints(second, midPointThroughSecond).head),
      second)
  }

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[Journey] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Journey = {
      val Array(imei, imsi, msisdn, id, startTime, endTime, geomWkt, cells, firstEventBeginTime, lastEventEndTime,
        numEvents, countryIsoCode) = fields

      Journey(
        user = User(imei = imei, imsi = imsi, msisdn = msisdn.toLong),
        id = id.toInt,
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

  implicit val fromRow = new RowParser[Journey] {

    override def fromRow(row: Row): Journey = {
      val Seq(Seq(imei, imsi, msisdn), id, startTime, endTime, geomWkt, cells, firstEventBeginTime, lastEventEndTime,
      numEvents, countryIsoCode) = row.toSeq

      Journey(
        user =
          User(imei = imei.asInstanceOf[String], imsi = imsi.asInstanceOf[String], msisdn = msisdn.asInstanceOf[Long]),
        id = id.asInstanceOf[Int],
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
