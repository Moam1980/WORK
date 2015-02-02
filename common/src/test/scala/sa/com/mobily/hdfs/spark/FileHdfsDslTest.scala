/*
 * TODO: License goes here!
 */

package sa.com.mobily.hdfs.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.hdfs.FileHdfs
import sa.com.mobily.utils.LocalSparkSqlContext

class FileHdfsDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import FileHdfsDsl._

  trait WithFileHdfsText {

    val fileHdfsText1 = "welcome-sms/0.3|csv|12.10.2014_09_50_TTS_2.LOG|2014/10/12 00:00:00|2014/12/05 18:24:17|" +
      "2014/12/05 18:24:17|134360"
    val fileHdfsText2 = "user-centric/jvp/0.6|csv|2014/11/29|part-00262|2015/01/22 05:09:27|2015/01/22 05:08:04|" +
      "68588092"
    val fileHdfsText3 = "cs/fileHdfs/0.3|parquet|2014/10/10|part-r-8.parquet|WRONG-DATE|2015/01/26 23:58:18|141914"

    val fileHdfs = sc.parallelize(List(fileHdfsText1, fileHdfsText2, fileHdfsText3))
  }

  trait WithFileHdfsRows {

    val row =
      Row("welcome-sms/0.3", "csv", "12.10.2014_09_50_TTS_2.LOG", 1413061200000L, 1417793057000L,
        1417793057000L, 134360L)
    val row2 =
      Row("user-centric/jvp/0.6", None, "part-00262", None, 1421892569000L, 1421892484000L,
        68588092L)
    val wrongRow =
      Row("cs/fileHdfs/0.3", Some("parquet"), "part-r-8.parquet", Some(1412888400000L), "WRONG-DATE", 1422305898000L,
        141914L)
    
    val rows = sc.parallelize(List(row, row2))
  }

  trait WithFileHdfs {

    val fileHdfs1 = FileHdfs(
      sourceName = "welcome-sms/0.3",
      fileFormat = Some("csv"),
      name = "12.10.2014_09_50_TTS_2.LOG",
      fileDate = Some(1413061200000L),
      modificationDate = 1417793057000L,
      accessTime = 1417793057000L,
      volumeBytes = 134360L)
    val fileHdfs2 = FileHdfs(
      sourceName = "user-centric/jvp/0.6",
      fileFormat = Some("csv"),
      name = "part-00262",
      fileDate = Some(1417208400000L),
      modificationDate = 1421892569000L,
      accessTime = 1421892484000L,
      volumeBytes = 68588092L)
    val fileHdfs3 = FileHdfs(
      sourceName = "cs/fileHdfs/0.3",
      fileFormat = Some("parquet"),
      name = "part-r-8.parquet",
      fileDate = Some(1412888400000L),
      modificationDate = 1422305905000L,
      accessTime = 1422305898000L,
      volumeBytes = 141914L)

    val filesHdfs = sc.parallelize(List(fileHdfs1, fileHdfs2, fileHdfs3))
  }

  "FileHdfsDsl" should "get correctly parsed data" in new WithFileHdfsText {
    fileHdfs.toFileHdfs.count should be (2)
  }

  it should "get errors when parsing data" in new WithFileHdfsText {
    fileHdfs.toFileHdfsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithFileHdfsText {
    fileHdfs.toParsedFileHdfs.count should be (3)
  }

  it should "get correctly parsed rows" in new WithFileHdfsRows {
    rows.toFileHdfs.count should be (2)
  }
  
  it should "save files hdfs in parquet" in new WithFileHdfs {
    val path = File.makeTemp().name
    filesHdfs.saveAsParquetFile(path)
    sqc.parquetFile(path).toFileHdfs.collect should be (filesHdfs.collect)
    File(path).deleteRecursively
  }

  it should "throw a exception if path doesn't exists" in new WithFileHdfs {
    an[Exception] should be thrownBy filesHdfs.toFilesHdfs("NotAPath")
  }
}
