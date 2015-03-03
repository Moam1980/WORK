import org.apache.spark.sql.SQLContext

import sa.com.mobily.cell.spark.CellDsl._
import sa.com.mobily.event.spark.EventDsl._
import sa.com.mobily.poi.spark.UserActivityDsl._
import sa.com.mobily.user.User

implicit val cellCatalogue = sc.textFile("${CELL_CATALOGUE}").toCell.toBroadcastMap
new SQLContext(sc).parquetFile("${source}").toEvent.toUserActivity.saveAsParquetFile("${destination}")
