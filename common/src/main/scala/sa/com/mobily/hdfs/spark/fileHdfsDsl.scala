/*
 * TODO: License goes here!
 */

package sa.com.mobily.hdfs.spark

import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.hdfs.FileHdfs
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class FileHdfsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedFileHdfs: RDD[ParsedItem[FileHdfs]] = SparkParser.fromCsv[FileHdfs](self)

  def toFileHdfs: RDD[FileHdfs] = toParsedFileHdfs.values

  def toFileHdfsErrors: RDD[ParsingError] = toParsedFileHdfs.errors
}

class FileHdfsRowReader(self: RDD[Row]) {

  def toFileHdfs: RDD[FileHdfs] = SparkParser.fromRow[FileHdfs](self)
}

class FileHdfsWriter(self: RDD[FileHdfs]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[FileHdfs](self, path)
}

trait FileHdfsDsl {

  implicit def fileHdfsReader(csv: RDD[String]): FileHdfsReader = new FileHdfsReader(csv)

  implicit def fileHdfsRowReader(self: RDD[Row]): FileHdfsRowReader = new FileHdfsRowReader(self)

  implicit def fileHdfsWriter(filesHdfs: RDD[FileHdfs]): FileHdfsWriter = new FileHdfsWriter(filesHdfs)
}

object FileHdfsDsl extends FileHdfsDsl with ParsedItemsDsl {

  def toFilesHdfs(path: String): Array[FileHdfs] =
    FileHdfs.parseFilesHdfs(path, FileSystem.get(new Configuration()).listFiles(new Path(path), true))
}
