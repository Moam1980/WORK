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
import scalax.chart.XYChart
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

  val HoursInWeek = 168
  val HoursInDay = 24
  val DaysInWeek = 7
  val SeriesPrefix = "Type "
  val GraphPrefix = "kmeans-graph-"
  val GraphMergedName = "kmeans-graphs-merged"
  val GraphSuffix = ".png"
  val WeekDaysLabel = "Week days"
  val WeekDays = Array("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
  val UserPhoneCallSeparator = ","

  private val GraphHorizontalSize = 2000
  private val GraphVerticalSize = 1000

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

  def kMeansModelGraphs(kMeansModel: KMeansModel, outputPath: String, plotCentroidsTogether: Boolean = false): Unit = {
    val centroidsData =
      for (centroid <- kMeansModel.clusterCenters; graphValues <- Seq(graphValues(centroid))) yield graphValues
    if (plotCentroidsTogether) plotCentroidsSameGraph(outputPath, centroidsData)
    else plotCentroidsDifferentGraphs(outputPath, centroidsData)
  }

  def plotCentroidsSameGraph(outputPath: String, centroidsData: Array[IndexedSeq[(Int, Double)]]): Unit = {
    val seriesNames = for (seriesIndex <- 1 to centroidsData.size) yield SeriesPrefix + seriesIndex
    val chartData = seriesNames zip centroidsData
    pngKMeansGraph(
      outputPath + File.separator + UserActivityCdr.GraphMergedName + UserActivityCdr.GraphSuffix,
      XYLineChart(chartData))
  }

  def plotCentroidsDifferentGraphs(outputPath: String, centroidsData: Array[IndexedSeq[(Int, Double)]]): Unit = {
    for (graphNumber <- 1 until centroidsData.length + 1)
      pngKMeansGraph(
        outputPath + File.separator + UserActivityCdr.GraphPrefix + graphNumber + UserActivityCdr.GraphSuffix,
        XYLineChart(centroidsData(graphNumber - 1)))
  }

  def graphValues(centroid: Vector): IndexedSeq[(Int, Double)] = {
    for (xValue <- 0 until centroid.size) yield (xValue, centroid(xValue))
  }

  def pngGraph(filePath: String, data: Seq[(Int, Double)]): Unit = {
    val chart = XYLineChart(data)
    chart.saveAsPNG(filePath)
  }

  def pngKMeansGraph(filePath: String, chart: XYChart): Unit = {
    val xAxis = new NumberAxis
    xAxis.setTickUnit(new NumberTickUnit(2))
    xAxis.setRange(0, HoursInWeek - 1)
    val symbolAxis = new SymbolAxis(WeekDaysLabel, WeekDays)
    chart.plot.setDomainAxes(Array(xAxis, symbolAxis))
    chart.saveAsPNG(filePath, (GraphHorizontalSize, GraphVerticalSize))
  }

  def weekHour(day: Int, hour: Int): Int = ((day - 1) * HoursInDay) + hour
}
