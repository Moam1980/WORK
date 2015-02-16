/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.language.implicitConversions

import com.github.nscala_time.time.Imports._
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.location.{Footfall, Location}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.poi.Poi
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.utils.EdmCoreUtils

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

  def matchDwell(
      dwells: RDD[Dwell],
      intervals: List[Interval],
      isMatch: (Geometry, Location) => Boolean = Location.isMatch,
      bestMatch: (Geometry, Seq[Location]) => Location = Location.bestMatch): RDD[((Location, Interval), Dwell)] = {
    val bcLocations = self.context.broadcast(self.collect.toList)
    dwells.flatMap(d => {
      val dwellIntervals = intervals.filter(i =>
        Option(new Interval(d.startTime, d.endTime, EdmCoreUtils.timeZone(d.countryIsoCode)).overlap(i)).isDefined)
      bcLocations.value.filter(l => isMatch(d.geom, l)) match {
        case Nil => Nil
        case onlyLocation :: Nil => dwellIntervals.map(i => ((onlyLocation, i), d))
        case severalLocations => dwellIntervals.map(i => ((bestMatch(d.geom, severalLocations), i), d))
      }
    })
  }

  def matchPoi(
      pois: RDD[Poi],
      isMatch: (Geometry, Location) => Boolean = Location.isMatch,
      bestMatch: (Geometry, Seq[Location]) => Location = Location.bestMatch): RDD[(Location, Poi)] = {
    val bcLocations = self.context.broadcast(self.collect.toList)
    pois.flatMap(p =>
      bcLocations.value.filter(l => isMatch(p.geometry, l)) match {
        case Nil => Nil
        case onlyLocation :: Nil => Seq((onlyLocation, p))
        case severalLocations => Seq((bestMatch(p.geometry, severalLocations), p))
      })
  }
}

class LocationTimeDwellFunctions(self: RDD[((Location, Interval), Dwell)]) {

  def profile: RDD[((Location, Interval), User)] = self.mapValues(_.user).distinct

  def footfall: RDD[((Location, Interval), Footfall)] = self.mapValues(Footfall(_)).reduceByKey(Footfall.aggregate)
}

trait LocationDsl {

  implicit def locationReader(csv: RDD[String]): LocationReader = new LocationReader(csv)

  implicit def locationFunctions(locations: RDD[Location]): LocationFunctions = new LocationFunctions(locations)

  implicit def locTimeDwellFunctions(locTimeDwell: RDD[((Location, Interval), Dwell)]): LocationTimeDwellFunctions =
    new LocationTimeDwellFunctions(locTimeDwell)
}

object LocationDsl extends LocationDsl with ParsedItemsDsl
