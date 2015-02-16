/*
 * TODO: License goes here!
 */

package sa.com.mobily.hdfs

import org.apache.hadoop.fs.{RemoteIterator, Path, LocatedFileStatus}
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.utils.{EdmCustomMatchers, LocalSparkContext}

case class RemoteFilesTest(files: Iterator[LocatedFileStatus]) extends RemoteIterator[LocatedFileStatus] {

  override def hasNext: Boolean = files.hasNext

  override def next: LocatedFileStatus = files.next
}

class FileHdfsTest extends FlatSpec with ShouldMatchers with LocalSparkContext with EdmCustomMatchers {

  import FileHdfs._

  trait WithFilesHdfs {

    val fileHdfsLine =
      "welcome-sms/0.3|csv|12.10.2014_09_50_TTS_2.LOG|2014/10/12 00:00:00|2014/12/05 18:24:17|2014/12/05 18:24:17|" +
        "134360"

    val fileHdfsHeader = Array("sourceName", "format", "name", "date", "modificationDate", "accessTime", "volumeBytes")
    val fileHdfsFields =
      Array("welcome-sms/0.3", "csv", "12.10.2014_09_50_TTS_2.LOG", "2014/10/12 00:00:00", "2014/12/05 18:24:17",
        "2014/12/05 18:24:17", "134360")

    val fields =
      Array("welcome-sms/0.3", "csv", "12.10.2014_09_50_TTS_2.LOG", "2014/10/12 00:00:00", "2014/12/05 18:24:17",
        "2014/12/05 18:24:17", "134360")

    val row =
      Row("welcome-sms/0.3", "csv", "12.10.2014_09_50_TTS_2.LOG", 1413061200000L, 1417793057000L,
        1417793057000L, 134360L)
    val wrongRow =
      Row("welcome-sms/0.3", "csv", "12.10.2014_09_50_TTS_2.LOG", 1413061200000L, "WRONG_FORMAT",
        1423031631000L, 134360L)

    val fileHdfs = FileHdfs(
      sourceName = "welcome-sms/0.3",
      fileFormat = Some("csv"),
      name = "12.10.2014_09_50_TTS_2.LOG",
      fileDate = Some(1413061200000L),
      modificationDate = 1417793057000L,
      accessTime = 1417793057000L,
      volumeBytes = 134360L)
  }

  trait WithLocatedFileStatus {
    val path = "/user/tdatuser/"

    val locatedFileStatusCompletePath =
      new LocatedFileStatus(134360L, false, 3, 134360L, 1417793057000L, 1417793057000L, null, "tdatuser", "tdatuser",
        null, new Path(path + "welcome-sms/0.3/2014/10/12/csv/12.10.2014_09_50_TTS_2.LOG"), null)
    val locatedFileStatusWithoutFormat =
      new LocatedFileStatus(134360L, false, 3, 134360L, 1417793057000L, 1417793057000L, null, "tdatuser", "tdatuser",
        null, new Path(path + "welcome-sms/0.3/2014/10/12/12.10.2014_09_50_TTS_2.LOG"), null)
    val locatedFileStatusWithoutDay =
      new LocatedFileStatus(134360L, false, 3, 134360L, 1417793057000L, 1417793057000L, null, "tdatuser", "tdatuser",
        null, new Path(path + "welcome-sms/0.3/2014/10/csv/12.10.2014_09_50_TTS_2.LOG"), null)
    val locatedFileStatusWithoutFormatAndDay =
      new LocatedFileStatus(134360L, false, 3, 134360L, 1417793057000L, 1417793057000L, null, "tdatuser", "tdatuser",
        null, new Path(path + "welcome-sms/0.3/2014/10/12.10.2014_09_50_TTS_2.LOG"), null)
    val locatedFileStatusWithoutDate =
      new LocatedFileStatus(134360L, false, 3, 134360L, 1417793057000L, 1417793057000L, null, "tdatuser", "tdatuser",
        null, new Path(path + "welcome-sms/0.3/12.10.2014_09_50_TTS_2.LOG"), null)


    val remoteIterator = RemoteFilesTest(
      Iterator[LocatedFileStatus](
        locatedFileStatusCompletePath,
        locatedFileStatusWithoutFormat,
        locatedFileStatusWithoutDay,
        locatedFileStatusWithoutFormatAndDay,
        locatedFileStatusWithoutDate))

    val fileHdfs = FileHdfs(
      sourceName = "welcome-sms/0.3",
      fileFormat = Some("csv"),
      name = "12.10.2014_09_50_TTS_2.LOG",
      fileDate = Some(1413061200000L),
      modificationDate = 1417793057000L,
      accessTime = 1417793057000L,
      volumeBytes = 134360L)
  }

  "FileHdfs" should "return correct header" in new WithFilesHdfs {
    FileHdfs.header should be (fileHdfsHeader)
  }

  it should "return correct fields" in new WithFilesHdfs {
    fileHdfs.fields should be (fileHdfsFields)
  }

  it should "return correct fields without format" in new WithFilesHdfs {
    fileHdfs.copy(fileFormat = None).fields should be (fileHdfsFields.updated(1, ""))
  }

  it should "return correct fields without date" in new WithFilesHdfs {
    fileHdfs.copy(fileDate = None).fields should be (fileHdfsFields.updated(3, ""))
  }

  it should "be built from CSV" in new WithFilesHdfs {
    CsvParser.fromLine(fileHdfsLine).value.get should be (fileHdfs)
  }

  it should "be discarded when the CSV format is wrong" in new WithFilesHdfs {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(6, "WrongNumber"))
  }

  it should "be built from Row" in new WithFilesHdfs {
    fromRow.fromRow(row) should be (fileHdfs)
  }

  it should "be discarded when row is wrong" in new WithFilesHdfs {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "parse complete to FileHdfs" in new WithLocatedFileStatus {
    FileHdfs.parseFileHdfs(path, locatedFileStatusCompletePath) should be (fileHdfs)
  }

  it should "parse without file format to FileHdfs" in new WithLocatedFileStatus {
    FileHdfs.parseFileHdfs(path, locatedFileStatusWithoutFormat) should
      be (fileHdfs.copy(fileFormat = None))
  }

  it should "parse without day to FileHdfs" in new WithLocatedFileStatus {
    FileHdfs.parseFileHdfs(path, locatedFileStatusWithoutDay) should
      be (fileHdfs.copy(fileDate = Some(1412110800000L)))
  }

  it should "parse without file format and file day to FileHdfs" in new WithLocatedFileStatus {
    FileHdfs.parseFileHdfs(path, locatedFileStatusWithoutFormatAndDay) should
      be (fileHdfs.copy(fileFormat = None, fileDate = Some(1412110800000L)))
  }

  it should "parse without date to FileHdfs" in new WithLocatedFileStatus {
    FileHdfs.parseFileHdfs(path, locatedFileStatusWithoutDate) should
      be (fileHdfs.copy(fileFormat = None, fileDate = None))
  }

  it should "parse all files FileHdfs" in new WithLocatedFileStatus {
    FileHdfs.parseFilesHdfs(path, remoteIterator).length should be (5)
  }
}
