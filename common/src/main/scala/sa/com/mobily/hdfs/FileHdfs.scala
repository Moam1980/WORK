/*
 * TODO: License goes here!
 */

package sa.com.mobily.hdfs

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.util.Try

import org.apache.hadoop.fs.{RemoteIterator, LocatedFileStatus}
import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.utils.EdmCoreUtils

case class FileHdfs(
    sourceName: String,
    fileFormat: Option[String],
    name: String,
    fileDate: Option[Long],
    modificationDate: Long,
    accessTime: Long,
    volumeBytes: Long) {

  def fields: Array[String] =
    Array(
      sourceName,
      fileFormat.getOrElse(""),
      name,
      fileDate.map(fileDate => EdmCoreUtils.parseTimestampToSaudiDate(fileDate)).getOrElse(""),
      EdmCoreUtils.parseTimestampToSaudiDate(modificationDate),
      EdmCoreUtils.parseTimestampToSaudiDate(accessTime),
      volumeBytes.toString)
}

object FileHdfs {

  final val CompleteFormat = """(.*?)/(\d\d\d\d)/(\d\d)/(\d\d)/(.*?)/(.*?)""".r
  final val FormatWithoutFileFormat = """(.*?)/(\d\d\d\d)/(\d\d)/(\d\d)/(.*?)""".r
  final val CompleteFormatWithoutDay = """(.*?)/(\d\d\d\d)/(\d\d)/(.*?)/(.*?)""".r
  final val FormatWithoutFileFormatAndDay = """(.*?)/(\d\d\d\d)/(\d\d)/(.*?)""".r

  def header: Array[String] =
    Array("sourceName", "format", "name", "date", "modificationDate", "accessTime", "volumeBytes")

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[FileHdfs]() {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): FileHdfs = {
      val Array(sourceNameText, formatText, nameText, dateText, modificationDateText, accessTimeText,
        volumeBytesText) = fields

      FileHdfs(
        sourceName = sourceNameText,
        fileFormat = EdmCoreUtils.parseString(formatText),
        name = nameText,
        fileDate = Try { EdmCoreUtils.Fmt.parseMillis(dateText) }.toOption,
        modificationDate = EdmCoreUtils.Fmt.parseMillis(modificationDateText),
        accessTime = EdmCoreUtils.Fmt.parseMillis(accessTimeText),
        volumeBytes = volumeBytesText.toLong)
    }
  }

  implicit val fromRow = new RowParser[FileHdfs] {

    override def fromRow(row: Row): FileHdfs = {
      val Seq(sourceName, fileFormat, name, fileDate, modificationDate, accessTime, volumeBytes) = row.toSeq

      FileHdfs(
        sourceName = sourceName.asInstanceOf[String],
        fileFormat = EdmCoreUtils.stringOption(fileFormat),
        name = name.asInstanceOf[String],
        fileDate = EdmCoreUtils.longOption(fileDate),
        modificationDate = modificationDate.asInstanceOf[Long],
        accessTime = accessTime.asInstanceOf[Long],
        volumeBytes = volumeBytes.asInstanceOf[Long])
    }
  }

  def parseFileHdfs(path: String, file: LocatedFileStatus): FileHdfs = { // scalastyle:ignore method.length
    val fileName = file.getPath.toString.substring(file.getPath.toString.indexOf(path) + path.length)

    // Pattern matching
    fileName match {
      case CompleteFormat(fileSource, year, month, day, fileFormat, fileName) =>
        FileHdfs(
          sourceName = fileSource,
          fileFormat = Some(fileFormat),
          name = fileName,
          fileDate = Some(EdmCoreUtils.Fmt.parseMillis(s"$year/$month/$day 00:00:00")),
          modificationDate = file.getModificationTime,
          accessTime = file.getAccessTime,
          volumeBytes = file.getLen)
      case FormatWithoutFileFormat(fileSource, year, month, day, fileName) =>
        FileHdfs(
          sourceName = fileSource,
          fileFormat = None,
          name = fileName,
          fileDate = Some(EdmCoreUtils.Fmt.parseMillis(s"$year/$month/$day 00:00:00")),
          modificationDate = file.getModificationTime,
          accessTime = file.getAccessTime,
          volumeBytes = file.getLen)
      case CompleteFormatWithoutDay(fileSource, year, month, fileFormat, fileName) =>
        FileHdfs(
          sourceName = fileSource,
          fileFormat = Some(fileFormat),
          name = fileName,
          fileDate = Some(EdmCoreUtils.Fmt.parseMillis(s"$year/$month/01 00:00:00")),
          modificationDate = file.getModificationTime,
          accessTime = file.getAccessTime,
          volumeBytes = file.getLen)
      case FormatWithoutFileFormatAndDay(fileSource, year, month, fileName) =>
        FileHdfs(
          sourceName = fileSource,
          fileFormat = None,
          name = fileName,
          fileDate = Some(EdmCoreUtils.Fmt.parseMillis(s"$year/$month/01 00:00:00")),
          modificationDate = file.getModificationTime,
          accessTime = file.getAccessTime,
          volumeBytes = file.getLen)
      case _ =>
        FileHdfs(
          sourceName = file.getPath.getParent.toString.substring(file.getPath.toString.indexOf(path) + path.length),
          fileFormat = None,
          name = file.getPath.getName,
          fileDate = None,
          modificationDate = file.getModificationTime,
          accessTime = file.getAccessTime,
          volumeBytes = file.getLen)
    }
  }

  def parseFilesHdfs(path: String, files: RemoteIterator[LocatedFileStatus]): Array[FileHdfs] = {
    val builder = new ArrayBuffer[FileHdfs]()

    while (files.hasNext) {
      val file = files.next()

      builder += FileHdfs.parseFileHdfs(path, file)
    }
    builder.toArray
  }
}
