import sa.com.mobily.xdr.spark.AiCsXdrDsl._
import sa.com.mobily.xdr.spark.IuCsXdrDsl._
import sa.com.mobily.event.spark.EventDsl._

sc.textFile("${origin}").${conversionMethod}.saveAsParquetFile("${destination}")

exit
