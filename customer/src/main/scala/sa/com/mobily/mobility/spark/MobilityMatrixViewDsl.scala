/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.mobility.MobilityMatrixView

class MobilityMatrixViewFunctions(self: RDD[MobilityMatrixView]) {

  def agg(another: RDD[MobilityMatrixView]): RDD[MobilityMatrixView] =
    self.union(another).keyBy(_.key).reduceByKey(MobilityMatrixView.aggregate).values

  def diff(another: RDD[MobilityMatrixView]): RDD[MobilityMatrixView] =
    self.union(another).keyBy(_.key).reduceByKey(MobilityMatrixView.difference).values
}

trait MobilityMatrixViewDsl {

  implicit def mobilityMatrixViewFunctions(mobilityMatrixView: RDD[MobilityMatrixView]): MobilityMatrixViewFunctions =
    new MobilityMatrixViewFunctions(mobilityMatrixView)
}

object MobilityMatrixViewDsl extends MobilityMatrixViewDsl
