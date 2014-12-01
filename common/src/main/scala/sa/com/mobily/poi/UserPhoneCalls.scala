/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.collection.Seq

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.sameersingh.scalaplot.Implicits._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class UserPhoneCalls(
    msisdn: Long,
    timestamp: DateTime,
    siteId: String,
    regionId: Long,
    callHours: Seq[Int])

object UserPhoneCalls {

  val DefaultMinActivityRatio = 0.1
  val HoursInWeek = 168

  final val UserPhoneCallSeparator = ","
  final val lineCsvParserObject = new OpenCsvParser(separator = ',', quote = '\'')

  implicit val fromCsv = new CsvParser[UserPhoneCalls] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): UserPhoneCalls = {
      val Array(msisdn, timestamp, siteId, regionId, callHours, _) = fields

      UserPhoneCalls(
        msisdn = msisdn.toLong,
        timestamp =
          DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime(timestamp),
        siteId = siteId,
        regionId = regionId.toLong,
        callHours = callHours.split(UserPhoneCallSeparator).map(hs => hs.trim.toInt))
    }
  }

  def generateClusterNumberAndCostSequence(data: RDD[Vector], maximumNumberOfClusters: Int): Seq[(Int, Double)] = {
    for (numberOfClusters <- 1 until maximumNumberOfClusters + 1)
    yield (numberOfClusters, computeCost(numberOfClusters, data))
  }

  def computeCost(k: Int, data: RDD[Vector]): Double = {
    val kMeans = new KMeans().setK(k)
    val kMeansModel = kMeans.run(data)
    kMeansModel.computeCost(data)
  }

  def generatePngGraph(dir: String, name: String, clusterNumberAndCosts: Seq[(Int, Double)]): String = {
    val xData = clusterNumberAndCosts.map(_._1.toDouble)
    val yData = clusterNumberAndCosts.map(_._2)
    output(PNG(dir, name), plot(xData -> yData))
  }
}
