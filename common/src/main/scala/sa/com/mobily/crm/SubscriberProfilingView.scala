/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class SubscriberProfilingView(
    imsi: String,
    category: ProfilingCategory) {

  def fields: Array[String] = imsi +: category.fields
}

case class ProfilingCategory(
    ageGroup: String,
    genderGroup: String,
    nationalityGroup: String,
    affluenceGroup: String) {

  def fields: Array[String] = Array(ageGroup, genderGroup, nationalityGroup, affluenceGroup)
}

object SubscriberProfilingView {

  val Header: Array[String] = "imsi" +: ProfilingCategory.Header

  def apply(imsi: String): SubscriberProfilingView =
    SubscriberProfilingView(
      imsi = imsi,
      category = ProfilingCategory(
        ageGroup = EdmCoreUtils.UnknownKeyword,
        genderGroup = EdmCoreUtils.UnknownKeyword,
        nationalityGroup =
          if (User.mcc(imsi) != CountryCode.SaudiArabiaMcc) ProfilingCategory.NationalityRoamers
          else EdmCoreUtils.UnknownKeyword,
        affluenceGroup = EdmCoreUtils.UnknownKeyword))

  implicit val fromCsv = new CsvParser[SubscriberProfilingView] {

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): SubscriberProfilingView = {
      val Array(imsi, ageGroup, genderGroup, nationalityGroup, affluenceGroup) = fields

      SubscriberProfilingView(
        imsi = imsi,
        ProfilingCategory(
          ageGroup = ageGroup,
          genderGroup = genderGroup,
          nationalityGroup = nationalityGroup,
          affluenceGroup = affluenceGroup))
    }
  }

  implicit val fromRow = new RowParser[SubscriberProfilingView] {

    override def fromRow(row: Row): SubscriberProfilingView = {
      val Row(imsi, Row(ageGroup, genderGroup, nationalityGroup, affluenceGroup)) = row

      SubscriberProfilingView(
        imsi = imsi.asInstanceOf[String],
        ProfilingCategory(
          ageGroup = ageGroup.asInstanceOf[String],
          genderGroup = genderGroup.asInstanceOf[String],
          nationalityGroup = nationalityGroup.asInstanceOf[String],
          affluenceGroup = affluenceGroup.asInstanceOf[String]))
    }
  }
}

object ProfilingCategory {

  val Header: Array[String] = Array("ageGroup", "genderGroup", "nationalityGroup", "affluenceGroup")

  val MaxTotalRevenue = 99999999

  val Age16To25 = "16-25"
  val Age26To60 = "26-60"
  val AgeGreaterThan60 = "61-"
  val AgeGroups = Array(Age16To25, Age26To60, AgeGreaterThan60, EdmCoreUtils.UnknownKeyword)

  def ageGroup(subscriber: Subscriber): String = subscriber.age.collect {
    case a if a >= 16 && a < 26 => Age16To25
    case a if a >= 26 && a <= 60 => Age26To60
    case a if a >= 61 => AgeGreaterThan60
    case _ => EdmCoreUtils.UnknownKeyword
  }.getOrElse(EdmCoreUtils.UnknownKeyword)

  val GenderMale = Male.id
  val GenderFemale = Female.id
  val GenderGroups = Array(GenderMale, GenderFemale, EdmCoreUtils.UnknownKeyword)

  def genderGroup(subscriber: Subscriber): String =
    if (subscriber.gender.equals(Male)) GenderMale
    else if (subscriber.gender.equals(Female)) GenderFemale
    else EdmCoreUtils.UnknownKeyword

  val NationalitySaudiArabia = "Saudi Arabia"
  val NationalityRoamers = "Roamers"
  val NationalityNonSaudi = "Non-Saudi"
  val NationalityGroups =
    Array(NationalitySaudiArabia, NationalityRoamers, NationalityNonSaudi, EdmCoreUtils.UnknownKeyword)

  def nationalityGroup(subscriber: Subscriber): String =
    if (subscriber.nationalities.inferred.toUpperCase == "SAUDI ARABIA") NationalitySaudiArabia
    else if (User.mcc(subscriber.user.imsi) != CountryCode.SaudiArabiaMcc) NationalityRoamers
    else NationalityNonSaudi

  def totalRevenue(subscriber: Subscriber): Float =
    if (subscriber.packages.corp == LargeCorporate || subscriber.types.pay == PostPaid) MaxTotalRevenue
    else subscriber.revenues.totalRevenue

  val AffluenceTop20 = "Top 20%"
  val AffluenceMiddle30 = "Middle 30%"
  val AffluenceBottom50 = "Bottom 50%"
  val affluenceGroups = Array(AffluenceTop20, AffluenceMiddle30, AffluenceBottom50, EdmCoreUtils.UnknownKeyword)

  def affluenceGroup(order: Long, max: Long): String = ((order * 100f) / max) match {
    case p if p > 80 => AffluenceTop20
    case p if p <= 80 && p > 50 => AffluenceMiddle30
    case p if p <= 50 => AffluenceBottom50
  }
}
