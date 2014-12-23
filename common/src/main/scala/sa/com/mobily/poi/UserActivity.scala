/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.collection.Seq

import org.apache.spark.mllib.linalg.{Vectors, Vector}

import sa.com.mobily.user.User

case class UserActivity(user: User, siteId: String, regionId: Short, activityVector: Vector) {

  lazy val key: (User, String, Short) = (user, siteId, regionId)
}

object UserActivity {

  def activityAverageVector(vectors: Seq[Vector]): Vector =
    Vectors.dense(zipWith(vectors.map(_.toArray))(seq => seq.sum / seq.size).toArray)

  def zipWith[A](activityArrays: Seq[Array[A]])(f: (Seq[A]) => A): List[A] = activityArrays.head.isEmpty match {
    case true => Nil
    case false => f(activityArrays.map(_.head)) :: (zipWith(activityArrays.map(_.tail))(f))
  }
}
