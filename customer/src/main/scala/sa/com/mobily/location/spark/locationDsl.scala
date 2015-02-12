/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.language.implicitConversions

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.location.Location
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class LocationReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedLocation: RDD[ParsedItem[Location]] = SparkParser.fromCsv[Location](self)

  def toLocation: RDD[Location] = toParsedLocation.values

  def toLocationErrors: RDD[ParsingError] = toParsedLocation.errors
}

class LocationFunctions(self: RDD[Location]) {

  def withTransformedGeom
      (longitudeFirstInCells: Boolean = true)
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[Location] = {
    val geomFactory = cellCatalogue.value.headOption.map(cellTuple => cellTuple._2.coverageGeom.getFactory).getOrElse(
      GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid))
    self.map(location =>
      location.copy(
        epsg = Coordinates.epsg(geomFactory.getSRID),
        geomWkt = GeomUtils.wkt(GeomUtils.transformGeom(location.geom, geomFactory, longitudeFirstInCells))))
  }

  def intersectingCells
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(Location, Seq[(Int, Int)])] = {
    val cellSeq = cellCatalogue.value.toSeq
    self.map(location => (location, cellSeq.collect { case (k, v) if v.coverageGeom.intersects(location.geom) => k }))
  }
}

trait LocationDsl {

  implicit def locationReader(csv: RDD[String]): LocationReader = new LocationReader(csv)

  implicit def locationFunctions(locations: RDD[Location]): LocationFunctions = new LocationFunctions(locations)
}

object LocationDsl extends LocationDsl with ParsedItemsDsl
