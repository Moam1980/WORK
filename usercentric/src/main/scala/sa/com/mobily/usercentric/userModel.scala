/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}

trait CountryGeometry {

  val geomWkt: String
  val countryIsoCode: String

  lazy val geom: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.isoCodeUtmSrid(countryIsoCode))
}

trait CellSequence {

  val orderedCells: List[(Int, Int)]

  lazy val cells: Set[(Int, Int)] = orderedCells.toSet
}

object UserModel {

  @tailrec
  def aggSameCell(
      events: List[Event],
      previous: Option[SpatioTemporalSlot] = None,
      result: List[SpatioTemporalSlot] = List())
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = events match {
    case Nil => result ++ previous
    case event :: tail if previous.isDefined =>
      if (previous.get.events.last.lacTac == event.lacTac && previous.get.events.last.cellId == event.cellId)
        aggSameCell(tail, Some(previous.get.append(event)), result)
      else
        aggSameCell(tail, Some(SpatioTemporalSlot(event)), result :+ previous.get)
    case event :: tail if !previous.isDefined => aggSameCell(tail, Some(SpatioTemporalSlot(event)), result)
  }
}
