/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils.spark

import scala.language.implicitConversions

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.utils.Stats


class StatsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedStats: RDD[ParsedItem[Stats]] = SparkParser.fromCsv[Stats](self)

  def toStats: RDD[Stats] = toParsedStats.values

  def toStatsErrors: RDD[ParsingError] = toParsedStats.errors
}

class StatsRowReader(self: RDD[Row]) {

  def toStats: RDD[Stats] = SparkParser.fromRow[Stats](self)
}

class StatsWriter(self: RDD[Stats]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Stats](self, path)
}

class StatsHelper {

  def computePercentile(indexedSortedRdd: RDD[(Long, Double)], count: Long, tile: Double): Double = {
    if (count == 1) indexedSortedRdd.first()._2
    else {
      val n = (tile / 100d) * (count + 1d)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) indexedSortedRdd.first()._2
      else {
        val last = count
        if (k >= count) {
          indexedSortedRdd.lookup(last - 1).head
        } else {
          indexedSortedRdd.lookup(k - 1).head +
            d * (indexedSortedRdd.lookup(k).head - indexedSortedRdd.lookup(k - 1).head)
        }
      }
    }
  }

  def toStats(rdd: RDD[Double]): Stats = {
    val sortedRdd = rdd.sortBy(x => x).cache
    val count = sortedRdd.count

    val stats = {
      if (count > 0) {
        val indexedSortedRdd = sortedRdd.zipWithIndex.map(_.swap).cache

        val stats = Stats(
          count = count,
          max = sortedRdd.max,
          min = sortedRdd.min,
          mean = sortedRdd.mean,
          variance = sortedRdd.variance,
          stDev = sortedRdd.stdev,
          percentile99 = computePercentile(indexedSortedRdd, count, 99D),
          percentile95 = computePercentile(indexedSortedRdd, count, 95D),
          percentile90 = computePercentile(indexedSortedRdd, count, 90D),
          percentile75 = computePercentile(indexedSortedRdd, count, 75D),
          percentile50 = computePercentile(indexedSortedRdd, count, 50D),
          percentile25 = computePercentile(indexedSortedRdd, count, 25D),
          percentile10 = computePercentile(indexedSortedRdd, count, 10D),
          percentile5 = computePercentile(indexedSortedRdd, count, 5D))

        indexedSortedRdd.unpersist(false)
        stats
      } else {
        Stats(Array[Double]())
      }
    }

    sortedRdd.unpersist(false)
    stats
  }
}

trait StatsDsl {

  implicit def statsReader(csv: RDD[String]): StatsReader = new StatsReader(csv)

  implicit def statsRowReader(self: RDD[Row]): StatsRowReader = new StatsRowReader(self)

  implicit def statsWriter(stats: RDD[Stats]): StatsWriter = new StatsWriter(stats)

  implicit def statsHelper: StatsHelper = new StatsHelper
}

object StatsDsl extends StatsDsl with ParsedItemsDsl
