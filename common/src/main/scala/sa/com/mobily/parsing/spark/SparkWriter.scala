
package sa.com.mobily.parsing.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.reflect.runtime.universe._

object SparkWriter {

  def  saveAsParquetFile[T<:Product:TypeTag](rdd: RDD[T],path:String): Unit =
    new SQLContext(rdd.sparkContext).createSchemaRDD(rdd).saveAsParquetFile(path)
}
