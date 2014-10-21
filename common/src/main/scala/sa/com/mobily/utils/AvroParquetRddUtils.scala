/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import scala.reflect.ClassTag

import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import parquet.avro.{AvroParquetOutputFormat, AvroReadSupport, AvroWriteSupport}
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

/**
 * Generic utility class for Avro Parquet
 */
object AvroParquetRddUtils {

  /**
   * Read in a parquet file containing Avro data and return the result as an RDD.
   *
   * @param sc The SparkContext to use
   * @param parquetFile The Parquet input file assumed to contain Avro objects
   * @return An RDD that contains the data of the file
   */
  def readParquetRdd[T <% SpecificRecord](sc: SparkContext, parquetFile: String)(implicit tag: ClassTag[T]): RDD[T] = {
    val jobConf = new JobConf(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(jobConf, classOf[AvroReadSupport[T]])
    sc.newAPIHadoopFile(
      parquetFile,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      tag.runtimeClass.asInstanceOf[Class[T]],
      jobConf)
      .map(_._2.asInstanceOf[T])
  }

  def writeParquetRdd[T <% SpecificRecord](
    sc: SparkContext,
    rdd: RDD[(Void, T)],
    schema: org.apache.avro.Schema,
    parquetDir: String)(implicit tag: ClassTag[T]) = {
    val jobConf = Job.getInstance(sc.hadoopConfiguration)
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(jobConf, classOf[AvroWriteSupport])
    // You need to pass the schema to AvroParquet when you are writing objects but not when you
    // are reading them. The schema is saved in Parquet file for future readers to use.
    AvroParquetOutputFormat.setSchema(jobConf, schema)
    // Save the RDD to a Parquet file in our temporary output directory

    rdd.saveAsNewAPIHadoopFile(parquetDir, classOf[Void], tag.runtimeClass.asInstanceOf[Class[T]],
      classOf[ParquetOutputFormat[T]], jobConf.getConfiguration)
  }
}
