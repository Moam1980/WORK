/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import scala.math._
import scala.util.Try

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.util.PolygonExtracter
import com.vividsolutions.jts.io.{WKTReader, WKTWriter}
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

object GeomUtils {

  val DefaultNumPoints = 50
  val PointInsideGeometryBufferFactor = 10
  val SimplifyGeomTolerance = 1
  val DefaultGeomBufferForIntersections = 0

  def geomFactory(srid: Integer, precisionModel: PrecisionModel = Coordinates.UtmPrecisionModel): GeometryFactory =
    new GeometryFactory(precisionModel, srid)

  def parseWkt(
      wkt: String,
      srid: Integer,
      precisionModel: PrecisionModel = Coordinates.UtmPrecisionModel): Geometry = {
    val factory = geomFactory(srid, precisionModel)
    new WKTReader(factory).read(wkt)
  }

  def wkt(geom: Geometry): String = new WKTWriter().write(geom)

  /**
   * @param longitudeFirst Whether to force for a projection with order (longitude, latitude). This projection order
   *                       (longitude, latitude) is not the standard in WGS84 (EPSG:4326) for instance, but most
   *                       GIS tools and systems (like QGIS and PostGIS) use it to make it similar to the cartesian
   *                       projection (x, y).
   */
  def transformGeom(
      geom: Geometry,
      destSrid: Int,
      destPrecisionModel: PrecisionModel,
      longitudeFirst: Boolean): Geometry = {
    val mathTransform = CRS.findMathTransform(
      CRS.decode(Coordinates.epsg(geom.getSRID)),
      CRS.decode(Coordinates.epsg(destSrid), longitudeFirst))
    val geomWithDestPrecisionModel = geomFactory(geom.getSRID, destPrecisionModel).createGeometry(geom)
    val transformedGeom = JTS.transform(geomWithDestPrecisionModel, mathTransform)
    transformedGeom.setSRID(destSrid)
    transformedGeom
  }

  def transformGeom(geom: Geometry, geomFactory: GeometryFactory, longitudeFirst: Boolean): Geometry =
    if (geom.getSRID == geomFactory.getSRID && geom.getPrecisionModel == geomFactory.getPrecisionModel) geom
    else transformGeom(geom, geomFactory.getSRID, geomFactory.getPrecisionModel, longitudeFirst)

  def circle(centre: Point, radius: Double, numPoints: Int = DefaultNumPoints): Geometry =
    geomShapeFactory(centre, radius, numPoints).createCircle

  /** Creates a circular sector polygon
    *
    * @param azimuth Angle in decimal degrees starting from the North pole and moving clockwise (i.e. azimuth)
    */
  def circularSector(
      position: Point,
      azimuth: Double,
      beamwidth: Double,
      radius: Double,
      numPoints: Int = DefaultNumPoints): Geometry = {
    val directionRad = toRadians(azimuthToAngle(azimuth % 360) - (beamwidth / 2))
    geomShapeFactory(position, radius, numPoints).createArcPolygon(directionRad, toRadians(beamwidth))
  }

  /** Creates a hippopede
    *
    * See [[http://en.wikipedia.org/wiki/Hippopede]]
    */
  def hippopede(
      location: Point,
      radius: Double,
      azimuth: Double,
      beamwidth: Double,
      numPoints: Int = DefaultNumPoints): Geometry = {
    val theta = toRadians(azimuthToAngle(azimuth % 360))
    val halfBeamwidthRad = toRadians(beamwidth) / 2
    val a = 1 / pow(sin(halfBeamwidthRad), 2)
    val stepAngle = 2 * halfBeamwidthRad / numPoints
    val angles = (1 to (numPoints - 1)).map(numPoint => (numPoint * stepAngle) - halfBeamwidthRad).toList
    val r = angles.map(angle => radius * sqrt(1 - a * pow(sin(angle), 2)))
    val rAngles = r.zip(angles)
    val x = rAngles.map(rAngle => rAngle._1 * cos(rAngle._2))
    val y = rAngles.map(rAngle => rAngle._1 * sin(rAngle._2))
    val xY = x.zip(y)
    val xSeq = xY.map(xY => location.getX + xY._1 * cos(theta) - xY._2 * sin(theta))
    val ySeq = xY.map(xY => location.getY + xY._1 * sin(theta) + xY._2 * cos(theta))
    val xCoords = location.getX +: xSeq :+ location.getX
    val yCoords = location.getY +: ySeq :+ location.getY

    buildShape(xCoords, yCoords, location.getSRID, location.getPrecisionModel)
  }

  /** Creates a conchoid
    *
    * See [[http://mathworld.wolfram.com/Conchoid.html]]
    */
  def conchoid(
      location: Point,
      radius: Double,
      azimuth: Double,
      beamwidth: Double,
      numPoints: Int = DefaultNumPoints): Geometry = {
    val theta = toRadians(azimuthToAngle(azimuth % 360))
    val halfBeamwidthRad = toRadians(beamwidth) / 2
    val a = -cos(halfBeamwidthRad)
    val stepAngle = 2 * halfBeamwidthRad / numPoints
    val angles = (1 to (numPoints - 1)).map(numPoint => (numPoint * stepAngle) - halfBeamwidthRad).toList
    val r = angles.map(angle => radius * (cos(angle) + a) / (1 + a))
    val rAngles = r.zip(angles)
    val x = rAngles.map(rAngle => rAngle._1 * cos(rAngle._2))
    val y = rAngles.map(rAngle => rAngle._1 * sin(rAngle._2))
    val xY = x.zip(y)
    val xSeq = xY.map(xY => location.getX + xY._1 * cos(theta) - xY._2 * sin(theta))
    val ySeq = xY.map(xY => location.getY + xY._1 * sin(theta) + xY._2 * cos(theta))
    val xCoords = location.getX +: xSeq :+ location.getX
    val yCoords = location.getY +: ySeq :+ location.getY

    buildShape(xCoords, yCoords, location.getSRID, location.getPrecisionModel)
  }

  def addBackLobe(mainLobe: Geometry, cellLocation: Point, range: Double, backLobeRatio: Double): Geometry =
    mainLobe.union(GeomUtils.circle(cellLocation, range * backLobeRatio))

  def azimuthToAngle(azimuth: Double): Double = 90 - azimuth

  def ensureNearestPointInGeom(p: Point, geom: Geometry): Point =
    if (p.intersects(geom)) p
    else if (!p.getPrecisionModel.isFloating)
      p.buffer(PointInsideGeometryBufferFactor / p.getPrecisionModel.getScale).intersection(geom).getCentroid
    else p.buffer(p.distance(geom) * PointInsideGeometryBufferFactor).intersection(geom).getCentroid

  def intersectionRatio(first: Geometry, second: Geometry): Double = {
    val firstArea = first.getArea
    val secondArea = second.getArea
    val intersectArea = largePrecision(first).intersection(largePrecision(second)).getArea
    if (firstArea >= secondArea) intersectArea / secondArea else intersectArea / firstArea
  }

  def geomAsPoints(geom: Geometry): Map[Int, (Double, Double)] =
    geom.getCoordinates.zipWithIndex.map { e => (e._2 + 1, (e._1.x, e._1.y)) }.toMap

  def largePrecision(geom: Geometry): Geometry =
    GeomUtils.geomFactory(geom.getSRID, new PrecisionModel).createGeometry(geom)

  private def buildShape(
      xCoords: List[Double],
      yCoords: List[Double],
      srid: Int,
      precisionModel: PrecisionModel): Geometry = {
    val coordinates = xCoords.zip(yCoords).map { xYCoord =>
      new Coordinate(precisionModel.makePrecise(xYCoord._1), precisionModel.makePrecise(xYCoord._2))
    }.toArray
    geomFactory(srid, precisionModel).createPolygon(coordinates)
  }

  private def geomShapeFactory(centre: Point, radius: Double, numPoints: Int): GeometricShapeFactory = {
    val factory = new GeometricShapeFactory(geomFactory(centre.getSRID, centre.getPrecisionModel))
    factory.setCentre(centre.getCoordinate)
    factory.setSize(2 * radius)
    factory.setNumPoints(numPoints)
    factory
  }

  def unionGeoms(geometries: Iterable[Geometry]): Geometry = {
    val firstGeom = geometries.head
    val remainingGeoms = geometries.tail
    val unionCandidate = remainingGeoms.foldLeft(firstGeom)((accumGeom, geom) =>
      Try { accumGeom.union(geom) }.toOption.getOrElse(accumGeom))
    firstGeom.getFactory.buildGeometry(PolygonExtracter.getPolygons(
      DouglasPeuckerSimplifier.simplify(unionCandidate, SimplifyGeomTolerance)))
  }

  def safeIntersects(geom1: Geometry, geom2: Geometry, buffer: Double = DefaultGeomBufferForIntersections): Boolean =
    Try { geom1.intersects(geom2) }.toOption.getOrElse(geom1.buffer(buffer).intersects(geom2.buffer(buffer)))

  def safeIntersection(geom1: Geometry, geom2: Geometry, buffer: Double = DefaultGeomBufferForIntersections): Geometry =
    Try { geom1.intersection(geom2) }.toOption.getOrElse(geom1.buffer(buffer).intersection(geom2.buffer(buffer)))
}
