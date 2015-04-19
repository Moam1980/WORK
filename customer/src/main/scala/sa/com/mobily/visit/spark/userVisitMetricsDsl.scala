/*
 * TODO: License goes here!
 */

package sa.com.mobily.visit.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.visit.{Footfall, UserVisitMetrics}

class UserVisitMetricsFunctions(self: RDD[UserVisitMetrics]) {

  def footfall: RDD[Footfall] = self.keyBy(_.key).mapValues(Footfall(_)).reduceByKey(Footfall.aggregate).values
}

trait UserVisitMetricsDsl {

  implicit def userVisitMetricsFunctions(userVisitMetrics: RDD[UserVisitMetrics]): UserVisitMetricsFunctions =
    new UserVisitMetricsFunctions(userVisitMetrics)
}

object UserVisitMetricsDsl extends UserVisitMetricsDsl
