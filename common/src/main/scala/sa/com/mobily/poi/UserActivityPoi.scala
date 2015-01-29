/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.util.Try

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.util.PolygonExtracter
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier

import sa.com.mobily.cell.EgBts
import sa.com.mobily.geometry.GeomUtils

object UserActivityPoi {

  def findGeometries(
      poiBtsIds: Iterable[(String, String)],
      btsCatalogue: Map[(String, String), Iterable[EgBts]]): Iterable[Geometry] = {
    poiBtsIds.flatMap(poiBtsId =>
      btsCatalogue.get((poiBtsId._1, poiBtsId._2)) match {
        case Some(location) => location.map(_.geom)
        case _ => Seq()
      }
    )
  }

  def unionGeoms(geometries: Iterable[Geometry]): Geometry = {
    val firstGeom = geometries.head
    val remainingGeoms = geometries.tail
    val unionCandidate = remainingGeoms.foldLeft(firstGeom)((accumGeom, btsGeom) =>
      Try { accumGeom.union(btsGeom) }.toOption.getOrElse(accumGeom))
    firstGeom.getFactory.buildGeometry(PolygonExtracter.getPolygons(
      DouglasPeuckerSimplifier.simplify(unionCandidate, GeomUtils.SimplifyGeomTolerance)))
  }
}
