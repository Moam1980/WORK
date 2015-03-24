/*
 * TODO: License goes here!
 */

package sa.com.mobily.user.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}
import sa.com.mobily.user.User

class UserRowReader(self: RDD[Row]) {

  def toUser: RDD[User] = SparkParser.fromRow[User](self)
}

class UserWriter(self: RDD[User]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[User](self, path)
}

trait UserDsl {

  implicit def userRowReader(self: RDD[Row]): UserRowReader = new UserRowReader(self)

  implicit def userWriter(users: RDD[User]): UserWriter = new UserWriter(users)
}

object UserDsl extends UserDsl
