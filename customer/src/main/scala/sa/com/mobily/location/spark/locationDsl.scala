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
import sa.com.mobily.location._
import sa.com.mobily.mobility.MobilityMatrixItem
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.poi.{Poi, PoiType, PoiMetrics}
import sa.com.mobily.poi.spark.PoiDsl
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.utils.{Stats, EdmCoreUtils}
import sa.com.mobily.utils.spark.StatsDsl
import sa.com.mobily.visit.UserVisitMetrics

class LocationReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedLocation: RDD[ParsedItem[Location]] = SparkParser.fromCsv[Location](self)

  def toLocation: RDD[Location] = toParsedLocation.values

  def toLocationErrors: RDD[ParsingError] = toParsedLocation.errors
}

class LocationFunctions(self: RDD[Location]) {

  import PoiDsl._

  def withTransformedGeom
      (longitudeFirstInCells: Boolean = true)
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[Location] = {
    val geomFactory = Cell.geomFactory(cellCatalogue.value)
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
        new Interval(d.startTime, d.endTime, EdmCoreUtils.timeZone(d.countryIsoCode)).overlaps(i))
      bcLocations.value.filter(l => isMatch(d.geom, l)) match {
        case Nil => Nil
        case onlyLocation :: Nil => dwellIntervals.map(i => ((onlyLocation, i), d))
        case severalLocations => dwellIntervals.map(i => ((bestMatch(d.geom, severalLocations), i), d))
      }
    })
  }

  def userVisitMetrics(
      dwells: RDD[Dwell],
      intervals: List[Interval],
      isMatch: (Geometry, Location) => Boolean = Location.isMatch,
      bestMatch: (Geometry, Seq[Location]) => Location = Location.bestMatch): RDD[UserVisitMetrics] = {
    val matchedDwells = matchDwell(dwells = dwells, intervals = intervals, isMatch = isMatch, bestMatch = bestMatch)
    val dwellPerLocationIntervalAndUser = matchedDwells.map(locIntDwell =>
      ((locIntDwell._1._1, locIntDwell._1._2, locIntDwell._2.user), List(locIntDwell._2)))
    dwellPerLocationIntervalAndUser.reduceByKey(_ ++ _).map(locIntUserDwells =>
      UserVisitMetrics(locIntUserDwells._1._1.name, locIntUserDwells._1._2, locIntUserDwells._2.sortBy(_.startTime)))
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

  def poiMetrics(matchedPoisLocations: RDD[(Location, Poi)]): RDD[(Location, LocationPoiMetrics)] = {
    self.sparkContext.parallelize(self.collect.map(l => (l, poiMetrics(l, matchedPoisLocations))))
  }


  def poiMetrics(location: Location, matchedPoisLocations: RDD[(Location, Poi)]): LocationPoiMetrics = {

    val matchedPoisLocationsFiltered = matchedPoisLocations.filter(_._1 == location)

    if (matchedPoisLocationsFiltered.count > 0) {
      val pois = matchedPoisLocationsFiltered.values
      val intersectionRatio = pois.map(p => GeomUtils.intersectionRatio(p.geometry, location.geom))

      LocationPoiMetrics(
        intersectionRatioStats = StatsDsl.statsHelper.toStats(intersectionRatio),
        poiMetrics = pois.poiMetrics)
    } else {
      LocationPoiMetrics(
        intersectionRatioStats = Stats(Array[Double]()),
        poiMetrics = PoiMetrics(
          numUsers = 0L,
          numPois = 0L,
          numUsersPerTypeCombination = Map[Seq[PoiType], Long](),
          distancePoisPerTypeCombination = Map[Seq[PoiType], Stats](),
          distancePoisSubPolygonsStats = Map[PoiType, Stats]()))
    }
  }

  def toMobilityMatrix(
      userDwells: RDD[(User, List[Dwell])],
      timeIntervals: List[Interval],
      minMinutesInDwell: Int): RDD[MobilityMatrixItem] = {
    val bcLocations = self.context.broadcast(self.collect.toList)
    userDwells.flatMap(dwells =>
      MobilityMatrixItem.perIntervalAndLocation(
        dwells = dwells._2,
        timeIntervals = timeIntervals,
        locations = bcLocations.value,
        minMinutesInDwell = minMinutesInDwell,
        numWeeks = EdmCoreUtils.numDifferentWeeksWithSundayFirstDay(timeIntervals)))
  }

  def toLocationPoiView(pois: RDD[Poi]): RDD[LocationPoiView] =
    matchPoi(pois).map(locationPoi => LocationPoiView(locationPoi._1, locationPoi._2))

  def toWeightedLocationPoiView(pois: RDD[Poi]): RDD[LocationPoiView] = {
    val bcLocations = self.context.broadcast(self.collect.toList)
    pois.flatMap(p => {
      val areaGeom = p.geometry.getArea
      bcLocations.value.filter(l => GeomUtils.safeIntersects(l.geom, p.geometry)).flatMap(l => {
        val areaIntersection = GeomUtils.safeIntersection(l.geom, p.geometry).getArea
        if (areaIntersection > 0D) Some(LocationPoiView(location = l, poi = p, weight = areaIntersection / areaGeom))
        else None
      })
    })
  }

  def distinctUsersPerDay(dwells: RDD[Dwell], minMinutesInDwell: Int): RDD[User] = {
    val unionGeom = self.map(_.geom).reduce((geom1, geom2) => GeomUtils.safeUnion(geom1, geom2))
    val filteredDwells =
      dwells.filter(_.durationInMinutes >= minMinutesInDwell).filter(d => GeomUtils.safeIntersects(d.geom, unionGeom))
    filteredDwells.map(d => (d.user, d.formattedDay)).distinct.keys
  }

  def withCentroids: RDD[Location] =
    self.map(location => location.copy(geomWkt = GeomUtils.wkt(location.geom.getCentroid)))

  def toWgs84LocationPointView: RDD[LocationPointView] =
    self.flatMap(location => LocationPointView.normalizedWgs84Geom(location))
}

trait LocationDsl {

  implicit def locationReader(csv: RDD[String]): LocationReader = new LocationReader(csv)

  implicit def locationFunctions(locations: RDD[Location]): LocationFunctions = new LocationFunctions(locations)
}

object LocationDsl extends LocationDsl with ParsedItemsDsl
