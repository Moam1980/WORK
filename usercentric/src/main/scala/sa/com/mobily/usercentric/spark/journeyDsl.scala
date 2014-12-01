/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import scala.language.implicitConversions

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.usercentric.Journey
import sa.com.mobily.utils.EdmCoreUtils

class JourneyFunctions(byUserChronologically: RDD[(Long, List[Event])]) {

  def withMinSpeeds(cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(Long, List[Event])] = {
    val eventsJoiningCellCatalogue = byUserChronologically.map(byUser => {
      val matchingEvents = byUser._2.filter(e => cellCatalogue.value.isDefinedAt((e.lacTac, e.cellId)))
      (byUser._1, matchingEvents)
    })
    eventsJoiningCellCatalogue.map(userEvents =>
      (userEvents._1, Journey.computeMinSpeed(userEvents._2, cellCatalogue.value)))
  }

  def segmentsAndGeometries(cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(Long, List[String])] = {
    val wkt = Event.geomWkt(cellCatalogue.value) _
    val geomFactory = cellCatalogue.value.headOption.map(cellTuple =>
      GeomUtils.geomFactory(cellTuple._2.coverageGeom.getSRID, cellTuple._2.coverageGeom.getPrecisionModel)).getOrElse(
        GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid))
    byUserChronologically.map { userEvents =>
      val minSpeedJourney = userEvents._2.sliding(2).collect {
        case List(first, second) if first.minSpeedPopulated && second.minSpeedPopulated =>
          val firstPoint =
            GeomUtils.parseWkt(first.minSpeedPointWkt.get, geomFactory.getSRID, geomFactory.getPrecisionModel)
          val secondPoint =
            GeomUtils.parseWkt(second.minSpeedPointWkt.get, geomFactory.getSRID, geomFactory.getPrecisionModel)
          val fields = Array(
            GeomUtils.wkt(geomFactory.createLineString(Array(firstPoint.getCoordinate, secondPoint.getCoordinate))),
            JourneyDsl.FmtDate.print(first.beginTime),
            JourneyDsl.FmtTime.print(first.beginTime) + "-" + JourneyDsl.FmtTime.print(second.beginTime) +
              " (" + EdmCoreUtils.roundAt1(first.outSpeed.get * JourneyDsl.KmPerHourInOneMeterPerSecond) + " km/h)",
            wkt(first),
            wkt(second))
          fields.mkString(EdmCoreUtils.Separator)
      }.toList
      (userEvents._1, minSpeedJourney)
    }
  }
}

trait JourneyDsl {

  implicit def journeyFunctions(byUserChronologically: RDD[(Long, List[Event])]): JourneyFunctions =
    new JourneyFunctions(byUserChronologically)
}

object JourneyDsl extends JourneyDsl {

  val KmPerHourInOneMeterPerSecond = 3.6
  val DateFormat = "yyyy/MM/dd"
  val TimeFormat = "HH:mm:ss"
  final val FmtDate = DateTimeFormat.forPattern(DateFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
  final val FmtTime = DateTimeFormat.forPattern(TimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
}
