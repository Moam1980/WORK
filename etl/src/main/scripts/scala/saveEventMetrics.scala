import org.apache.spark.sql.SQLContext

import sa.com.mobily.event.spark.EventDsl._

new SQLContext(sc).parquetFile("${source}").toEvent.saveMetrics("${destination}")

exit
