/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.collection.immutable.IndexedSeq
import scala.collection.Seq

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scalax.chart.api._

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
  val HoursInDay = 24
  val DaysInWeek = 7
  val KMeansGraphPrefix = "kmeans-graph-"

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
    val model = kMeansModel(k, data)
    model.computeCost(data)
  }

  def kMeansModel(k: Int, data: RDD[Vector]): KMeansModel = {
    val kMeans = new KMeans().setK(k)
    kMeans.run(data)
  }

  def kMeansModelGraphs(kMeansModel: KMeansModel, outputPath: String): Unit = {
    val modelGraphs = for (centroid <- kMeansModel.clusterCenters;
      graphValues <- Seq(graphValues(centroid))) yield (centroid, graphValues)
    for (graphNumber <- 0 until modelGraphs.length)
      pngGraph(outputPath.concat(KMeansGraphPrefix + graphNumber), modelGraphs(graphNumber)._2)
  }

  def graphValues(centroid: Vector): IndexedSeq[(Int, Double)] = {
    for (xValue <- 0 until centroid.size) yield (xValue, centroid(xValue))
  }

  def pngGraph(filePath: String, data: Seq[(Int, Double)]): Unit = {
    val chart = XYLineChart(data)
    chart.saveAsPNG(filePath)
  }

  def weekHour(day: Int, hour: Int): Int = ((day - 1) * HoursInDay) + hour
}
