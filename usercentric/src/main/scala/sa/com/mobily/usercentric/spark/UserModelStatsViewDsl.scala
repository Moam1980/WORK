/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.usercentric.UserModelStatsView

class UserModelStatsViewWriter(self: RDD[UserModelStatsView]) {

  def saveAsCsv(path: String): Unit = self.map(_.toCsvFields).saveAsTextFile(path)
}

trait UserModelStatsViewDsl {

  implicit def subscriberViewWriter(self: RDD[UserModelStatsView]): UserModelStatsViewWriter =
    new UserModelStatsViewWriter(self)
}

object UserModelStatsViewDsl extends UserModelStatsViewDsl
