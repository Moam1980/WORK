/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import scala.reflect.io.File

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.usercentric.{TotalNumberOfUsersByDwells, TotalNumberOfDwells, UserModelStatsView}
import sa.com.mobily.utils.LocalSparkSqlContext

class UserModelStatsViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import UserModelStatsViewDsl._

  trait WithUserModelStatsView {

    val userModelStatsView1 = UserModelStatsView(TotalNumberOfDwells, "01/01/2010", 5.0)
    val userModelStatsView2 = UserModelStatsView(TotalNumberOfUsersByDwells, "02/01/2010", 7.0)
    val userModelStats = sc.parallelize(Array(userModelStatsView1, userModelStatsView2))

    def withTemporaryFile(testFunction: File => Any) {
      val file = File.makeTemp()
      testFunction(file)
      File(file.name).deleteRecursively
    }
  }

  "UserModelStatsViewDsl" should "save an RDD of UserModelStatsView as text file" in new WithUserModelStatsView {
    withTemporaryFile { file =>
      userModelStats.saveAsCsv(file.name)
      val textFile = sc.textFile(file.name).collect.toSeq
      textFile.size should be (2)
      textFile should contain("TotalNumberOfDwells|01/01/2010|5.0")
      textFile should contain("TotalNumberOfUsersByDwells|02/01/2010|7.0")
    }
  }
}
