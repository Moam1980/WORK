/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.mobility.MobilityMatrixItem

class MobilityMatrixFunctions(self: RDD[MobilityMatrixItem]) {
}

trait MobilityMatrixDsl {

  implicit def mobilityMatrixFunctions(mobilityMatrix: RDD[MobilityMatrixItem]): MobilityMatrixFunctions =
    new MobilityMatrixFunctions(mobilityMatrix)
}

object MobilityMatrixDsl extends MobilityMatrixDsl
