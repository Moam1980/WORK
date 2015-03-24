/*
 * TODO: License goes here!
 */

package sa.com.mobily.user.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class UserDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import UserDsl._

  trait WithUserRows {

    val row = Row("866173010386736", "420034122616618", 560917079L)
    val row2 = Row("OTHER_IMEI", "OTHER_IMSI", 966540093335L)
    val wrongRow = Row("866173010386736", "420034122616618", "560917079L")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithUser {
    
    val user1 = User(
      imei = "866173010386736",
      imsi = "420034122616618",
      msisdn = 560917079L)

    val user2 = User(
      imei = "OTHER_IMEI",
      imsi = "OTHER_IMSI",
      msisdn = 966540093335L)

    val users = sc.parallelize(List(user1, user2))
  }

  "UserDsl" should "get correctly parsed rows" in new WithUserRows {
    rows.toUser.count should be (2)
  }

  it should "save in parquet" in new WithUser {
    val path = File.makeTemp().name
    users.saveAsParquetFile(path)
    sqc.parquetFile(path).toUser.collect should be (users.collect)
    File(path).deleteRecursively
  }
}
