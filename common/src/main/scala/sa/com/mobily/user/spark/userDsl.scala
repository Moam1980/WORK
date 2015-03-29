/*
 * TODO: License goes here!
 */

package sa.com.mobily.user.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
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

class UserFunctions(self: RDD[User]) {

  def withMinOccurrences(minOccurrences: Int): RDD[User] = {
    require(minOccurrences > 0)
    self.map(u => (u, 1)).reduceByKey(_ + _).filter(_._2 >= minOccurrences).keys
  }
}

trait UserDsl {

  implicit def userRowReader(self: RDD[Row]): UserRowReader = new UserRowReader(self)

  implicit def userWriter(users: RDD[User]): UserWriter = new UserWriter(users)

  implicit def userFunctions(users: RDD[User]): UserFunctions = new UserFunctions(users)
}

object UserDsl extends UserDsl
