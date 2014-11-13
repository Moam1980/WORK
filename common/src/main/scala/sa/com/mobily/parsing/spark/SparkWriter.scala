/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing.spark

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object SparkWriter {

  def saveAsParquetFile[T <: Product : TypeTag](rdd: RDD[T], path: String): Unit =
    new SQLContext(rdd.sparkContext).createSchemaRDD(rdd).saveAsParquetFile(path)
}
