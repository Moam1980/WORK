import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import sa.com.mobily.event.Event
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.spark.JourneyDsl._
import sa.com.mobily.usercentric.spark.UserModelDsl._
import sa.com.mobily.utils.EdmCoreUtils
import sa.com.mobily.xdr.spark.AiCsXdrDsl._
import sa.com.mobily.xdr.spark.IuCsXdrDsl._
import sa.com.mobily.xdr.spark.UfdrPsXdrDsl._

implicit val cellCatalogue = sc.textFile("${CELL_CATALOGUE}").toCell.toBroadcastMap
val events = new SQLContext(sc).parquetFile("${source}").toEvent
val model = events.withMatchingCell.byUserChronologically.withMinSpeeds.aggTemporalOverlapAndSameCell.toUserCentric
  .persist(StorageLevel.MEMORY_AND_DISK)
events.unpersist()
cellCatalogue.unpersist()
model.flatMap(userModel => userModel._2._1).saveAsParquetFile("${dwell}")
model.flatMap(userModel => userModel._2._2).saveAsParquetFile("${journey}")
model.flatMap(userModel => userModel._2._3).saveAsParquetFile("${jvp}")

