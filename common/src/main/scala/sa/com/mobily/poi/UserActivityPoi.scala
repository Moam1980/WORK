/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.cell.EgBts

object UserActivityPoi {

  def findGeometries(
      poiBtsIds: Iterable[(String, Short)],
      btsCatalogue: Map[(String, Short), Iterable[EgBts]]): Iterable[Geometry] = {
    poiBtsIds.flatMap(poiBtsId =>
      btsCatalogue.get((poiBtsId._1, poiBtsId._2)) match {
        case Some(location) => location.map(_.geom)
        case _ => Seq()
      }
    )
  }
}
