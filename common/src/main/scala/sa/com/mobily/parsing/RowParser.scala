/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing

import org.apache.spark.sql.Row

abstract class RowParser[T] extends Serializable {

  def fromRow(row: Row): T
}

object RowParser {

  def fromRow[T](row: Row)(implicit rowParser: RowParser[T]): T = rowParser.fromRow(row)

  def fromRows[T](rows: Iterator[Row])(implicit rowParser: RowParser[T]): Iterator[T] = rows.map(row => fromRow(row))
}
