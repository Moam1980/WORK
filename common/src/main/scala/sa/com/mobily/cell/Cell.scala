/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.CsvParser

case class Cell(cellId: Long, lat: Double, long: Double)

object Cell {

  implicit val fromCsv = new CsvParser[Cell] {

    override def fromFields(fields: Array[String]): Cell = {
      val Array(cellId, lat, long) = fields
      Cell(cellId.toLong, lat.toDouble, long.toDouble)
    }
  }
}
