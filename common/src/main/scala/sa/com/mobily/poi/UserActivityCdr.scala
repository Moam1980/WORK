/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import java.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.jfree.chart.axis.{NumberAxis, NumberTickUnit, SymbolAxis}
import scalax.chart.api._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class UserActivityCdr(
    user: User,
    timestamp: DateTime,
    siteId: String,
    regionId: Short,
    activityHours: Seq[Int])

object UserActivityCdr {

  val DefaultMinActivityRatio = 0.1
  val HoursInWeek = 168
  val HoursInDay = 24
  val DaysInWeek = 7
  val GraphPrefix = "kmeans-graph-"
  val GraphSuffix = ".png"
  val WeekDaysLabel = "Week days"
  val WeekDays = Array("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  private val GraphHorizontalSize = 2000
  private val GraphVerticalSize = 1000

  final val UserPhoneCallSeparator = ","
  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[UserActivityCdr] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): UserActivityCdr = {
      val Array(msisdn, timestamp, siteId, regionId, activityHours) = fields

      UserActivityCdr(
        user = User("", "", msisdn.toLong),
        timestamp =
          DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime(timestamp),
        siteId = siteId,
        regionId = regionId.toShort,
        activityHours = activityHours.split(UserPhoneCallSeparator).map(hs => hs.trim.toInt))
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
      graphValues <- Seq(graphValues(centroid))) yield graphValues
    for (graphNumber <- 1 until modelGraphs.length + 1)
      pngKMeansGraph(
        outputPath + File.separator + UserActivityCdr.GraphPrefix + graphNumber + UserActivityCdr.GraphSuffix,
        modelGraphs(graphNumber - 1))
  }

  def graphValues(centroid: Vector): IndexedSeq[(Int, Double)] = {
    for (xValue <- 0 until centroid.size) yield (xValue, centroid(xValue))
  }

  def pngGraph(filePath: String, data: Seq[(Int, Double)]): Unit = {
    val chart = XYLineChart(data)
    chart.saveAsPNG(filePath)
  }

  def pngKMeansGraph(filePath: String, data: Seq[(Int, Double)]): Unit = {
    val xAxis = new NumberAxis
    xAxis.setTickUnit(new NumberTickUnit(2))
    xAxis.setRange(0, HoursInWeek - 1)
    val symbolAxis = new SymbolAxis(WeekDaysLabel, WeekDays)
    val chart = XYLineChart(data)
    chart.plot.setDomainAxes(Array(xAxis, symbolAxis))
    chart.saveAsPNG(filePath, (GraphHorizontalSize, GraphVerticalSize))
  }

  def weekHour(day: Int, hour: Int): Int = ((day - 1) * HoursInDay) + hour
}
