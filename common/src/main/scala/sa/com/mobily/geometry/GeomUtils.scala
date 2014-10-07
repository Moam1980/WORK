/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import scala.math._

import org.geotools.geometry.jts.JTSFactoryFinder
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.util.GeometricShapeFactory

object GeomUtils {

  val DefaultNumPoints = 50

  def parseWkt(wkt: String, srid: Integer): Geometry = {
    val geomFactory = JTSFactoryFinder.getGeometryFactory
    val wktReader = new WKTReader(geomFactory)
    val geom = wktReader.read(wkt)
    geom.setSRID(srid)
    geom
  }

  def circle(centre: Point, radius: Double, numPoints: Int = DefaultNumPoints): Geometry = {
    val factory = geomShapeFactory(centre, radius, numPoints)
    val circle = factory.createCircle
    circle.setSRID(centre.getSRID)
    circle
  }

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
    val factory = geomShapeFactory(position, radius, numPoints)
    val directionRad = toRadians(azimuthToAngle(azimuth % 360) - (beamwidth / 2))
    val poly = factory.createArcPolygon(directionRad, toRadians(beamwidth))
    poly.setSRID(position.getSRID)
    poly
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

    buildShape(xCoords, yCoords, location.getSRID)
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

    buildShape(xCoords, yCoords, location.getSRID)
  }

  def addBackLobe(mainLobe: Geometry, cellLocation: Point, range: Double, backLobeRatio: Double): Geometry = {
    val geomUnion = mainLobe.union(GeomUtils.circle(cellLocation, range * backLobeRatio))
    geomUnion.setSRID(mainLobe.getSRID)
    geomUnion
  }

  def azimuthToAngle(azimuth: Double): Double = 90 - azimuth

  private def buildShape(xCoords: List[Double], yCoords: List[Double], srid: Int): Geometry = {
    val coordinates = xCoords.zip(yCoords).map(xYCoord => new Coordinate(xYCoord._1, xYCoord._2)).toArray
    val poly = JTSFactoryFinder.getGeometryFactory.createPolygon(coordinates)
    poly.setSRID(srid)
    poly
  }

  private def geomShapeFactory(centre: Point, radius: Double, numPoints: Int): GeometricShapeFactory = {
    val geomShapeFactory = new GeometricShapeFactory
    geomShapeFactory.setCentre(centre.getCoordinate)
    geomShapeFactory.setSize(2 * radius)
    geomShapeFactory.setNumPoints(numPoints)
    geomShapeFactory
  }
}
