/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.ImsBts

class ImsBtsFunctions(self: RDD[ImsBts]) {

  def mergeImsBts: RDD[ImsBts] =
    self.map(imsBts => ((imsBts.vendor, imsBts.technology, imsBts.id), imsBts)).reduceByKey(ImsBts.merge).map(_._2)
}

trait ImsBtsDsl {

  implicit def imsBtsFunctions(imsBts: RDD[ImsBts]): ImsBtsFunctions = new ImsBtsFunctions(imsBts)
}

object ImsBtsDsl extends ImsBtsDsl
