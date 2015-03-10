package sa.com.mobily.utils.spark

import scala.reflect.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.Row
import org.scalatest._

import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkSqlContext}

case class Person(id: Int, name: String)

class RddRowDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import RddRowDsl._

  trait WithDatesAndPaths {
    val Fmt = DateTimeFormat.forPattern("yyyy/MM/dd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
    val baseDT = new DateTime().withZone(EdmCoreUtils.TimeZoneSaudiArabia).withTime(0, 0, 0, 0)
    val dt1 = baseDT.withDay(19).withMonth(7).withYear(2011)
    val dt2 = baseDT.withDay(26).withMonth(7).withYear(2011)
    val dateString1 = "2011/07/19"
    val dateString2 = "2011/07/26"
    val wrongDate1 = "2011/29/02"
    val wrongDate2 = "2011/29/as"
    val fakePath = "fake/path/to/test/2015/01/01/parquet"
    val fakePathResult1 = s"fake/path/to/test/$dateString1/parquet"
    val fakePathResult2 = s"fake/path/to/test/$dateString2/parquet"
    val wrongPathResult1 = s"fake/path/to/test/$wrongDate1/parquet"
    val wrongPathResult2 = s"fake/path/to/test/$wrongDate2/parquet"
    val initPath = "/2015/01/01"
    val path1 = "/" + dateString1 + "/parquet"
    val path2 = "/" + dateString2 + "/parquet"
  }

  trait WithRows extends WithDatesAndPaths {

    def save(i: Int, path: String): Seq[Person] = {
      val data = generateRamdomData
      val rdd = sc.parallelize(data)
      sqc.createSchemaRDD(rdd).saveAsParquetFile(RddRowDsl.replaceDate(path, dt1.plusDays(i)))
      data
    }

    def generateRamdomData: Seq[Person] = {
      val r = scala.util.Random
      for (i <- 0 to r.nextInt(100)) yield Person(r.nextInt(100), r.nextString(5))
    }
  }

  "RddRowDsl" should "convert dates from string to DateTime" in new WithDatesAndPaths {
    RddRowDsl.getDateFromString(dateString1).get should be (dt1)
    RddRowDsl.getDateFromString(dateString2).get should be (dt2)
    RddRowDsl.getDateFromString(wrongDate1).getOrElse(None) should be (None)
    RddRowDsl.getDateFromString(wrongDate2).getOrElse(None) should be (None)
  }

  it should "convert dates from path to DateTime" in new WithDatesAndPaths {
    RddRowDsl.getDateFromPath(fakePathResult1).get should be (dt1)
    RddRowDsl.getDateFromPath(fakePathResult2).get should be (dt2)
    RddRowDsl.getDateFromPath(wrongPathResult1).getOrElse(None) should be (None)
    RddRowDsl.getDateFromPath(wrongPathResult2).getOrElse(None) should be (None)
  }

  it should "replace path with current dates" in new WithDatesAndPaths {
    RddRowDsl.replaceDate(fakePath, dt1) should be (fakePathResult1)
    RddRowDsl.replaceDate(fakePath, dt2) should be (fakePathResult2)
  }

  it should "read from period with dates" in new WithRows {
    val tmpPath = File.makeTemp().name
    val path = tmpPath + path1
    val data = (0 to 7).map(i => save(i, path))
    val rows = data.foldLeft(Seq[Person]())(_ ++ _).map(f => Row(f.id, f.name))
    sqc.readFromPeriod(path, dt1, dt2).collect.sameElements(rows) should be (true)
    File(tmpPath).deleteRecursively
  }

  it should "read from period with string dates" in new WithRows {
    val tmpPath = File.makeTemp().name
    val path = tmpPath + path1
    val data = (0 to 7).map(i => save(i, path))
    val rows = data.foldLeft(Seq[Person]())(_ ++ _).map(f => Row(f.id, f.name))
    sqc.readFromPeriod(path, dateString1, dateString2).collect.sameElements(rows) should be (true)
    File(tmpPath).deleteRecursively
  }
}
