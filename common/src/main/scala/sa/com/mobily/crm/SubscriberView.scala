/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm

import scala.language.existentials

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.utils.EdmCoreUtils

case class SubscriberView(
    imsi: String,
    age: Option[Float],
    gender: String,
    nationalities: Nationalities,
    types: SubscriberTypes,
    packages: SubscriberPackages,
    date: SubscriberDates,
    activeStatus: ActiveStatus,
    currentBalance: Option[Float],
    calculatedSegment: CalculatedSegment,
    revenues: Revenues) {

  def fields: Array[String] = Array(
    imsi,
    age.getOrElse("").toString,
    gender,
    nationalities.declared,
    nationalities.inferred,
    types.pay.id,
    types.handset,
    packages.data.id,
    packages.corp.id,
    date.activation.getOrElse("").toString,
    date.lastActivity.getOrElse("").toString,
    date.lastRecharge.getOrElse("").toString,
    activeStatus.id,
    currentBalance.getOrElse("").toString,
    calculatedSegment.id,
    revenues.m1.toString,
    revenues.m2.toString,
    revenues.m3.toString,
    revenues.m4.toString,
    revenues.m5.toString,
    revenues.m6.toString)
}

object SubscriberView {

  def apply(subscriber: Subscriber): SubscriberView =
    SubscriberView(
      subscriber.user.imsi,
      subscriber.age,
      subscriber.gender,
      subscriber.nationalities,
      subscriber.types,
      subscriber.packages,
      subscriber.date,
      subscriber.activeStatus,
      subscriber.currentBalance,
      subscriber.calculatedSegment,
      subscriber.revenues)

  def header: Array[String] = Array("imsi", "age", "gender", "nationality-declared", "nationality-inferred", "paytype",
    "handsetType", "packages-data", "packages-corp", "activation-date", "last-activity-date", "last-recharge-date",
    "active-status", "currentBalance", "calculated-segment", "revenues-m1", "revenues-m2", "revenues-m3", "revenues-m4",
    "revenues-m5", "revenues-m6")

  implicit val fromCsv = new CsvParser[SubscriberView] {

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): SubscriberView = {
      val Array(imsi, age, gender, natDeclared, natInferred, pay, handset, packData, packCorp, dateActivation,
        dateLastActivity, dateLastRecharge, activeStatus, currentBalance, calculatedSegment, revenuesM1, revenuesM2,
        revenuesM3, revenuesM4, revenuesM5, revenuesM6) = fields

      SubscriberView(
        imsi = imsi,
        age = EdmCoreUtils.parseFloat(age),
        gender = gender,
        nationalities = Nationalities(natDeclared.toUpperCase, natInferred.toUpperCase),
        types = SubscriberTypes(Subscriber.parsePayType(pay), handset),
        packages = SubscriberPackages(Subscriber.parseDataPackage(packData), Subscriber.parseCorpPackage(packCorp)),
        date = SubscriberDates(
          activation = EdmCoreUtils.parseLong(dateActivation),
          lastActivity = EdmCoreUtils.parseLong(dateLastActivity),
          lastRecharge = EdmCoreUtils.parseLong(dateLastRecharge)),
        activeStatus = Subscriber.parseActiveStatus(activeStatus),
        currentBalance = EdmCoreUtils.parseFloat(currentBalance),
        calculatedSegment = Subscriber.parseCalculatedSegment(calculatedSegment),
        revenues = Revenues(
          m1= revenuesM1.toFloat,
          m2 = revenuesM2.toFloat,
          m3 = revenuesM3.toFloat,
          m4 = revenuesM4.toFloat,
          m5 = revenuesM5.toFloat,
          m6 = revenuesM6.toFloat))
    }
  }

  implicit val fromRow = new RowParser[SubscriberView] {

    override def fromRow(row: Row): SubscriberView = {
      val Row(
        imsi,
        age,
        gender,
        nationalitiesRow,
        typesRow,
        packagesRow,
        dateRow,
        activeStatusRow,
        currentBalance,
        calculatedSegmentRow,
        revenuesRow) = row
      val Row(declared, inferred) = nationalitiesRow.asInstanceOf[Row]
      val Row(Row(pay), handset) = typesRow.asInstanceOf[Row]
      val Row(Row(data), Row(corp)) = packagesRow.asInstanceOf[Row]
      val Row(activation, lastActivity, lastRecharge) = dateRow.asInstanceOf[Row]
      val Row(activeStatus) = activeStatusRow.asInstanceOf[Row]
      val Row(calculatedSegment) = calculatedSegmentRow.asInstanceOf[Row]
      val Row(m1, m2, m3 , m4, m5, m6) = revenuesRow.asInstanceOf[Row]

      SubscriberView(
        imsi = imsi.asInstanceOf[String],
        age = EdmCoreUtils.floatOption(age),
        gender = gender.asInstanceOf[String],
        nationalities = Nationalities(declared.asInstanceOf[String], inferred.asInstanceOf[String]),
        types = SubscriberTypes(PayType(pay.asInstanceOf[String]), handset.asInstanceOf[String]),
        packages = SubscriberPackages(DataPackage(data.asInstanceOf[String]), CorpPackage(corp.asInstanceOf[String])),
        date = SubscriberDates(
          activation = EdmCoreUtils.longOption(activation),
          lastActivity = EdmCoreUtils.longOption(lastActivity),
          lastRecharge = EdmCoreUtils.longOption(lastRecharge)),
        activeStatus = ActiveStatus(activeStatus.asInstanceOf[String]),
        currentBalance = EdmCoreUtils.floatOption(currentBalance),
        calculatedSegment = CalculatedSegment(calculatedSegment.asInstanceOf[String]),
        revenues = Revenues(
          m1 = EdmCoreUtils.floatOrZero(m1),
          m2 = EdmCoreUtils.floatOrZero(m2),
          m3 = EdmCoreUtils.floatOrZero(m3),
          m4 = EdmCoreUtils.floatOrZero(m4),
          m5 = EdmCoreUtils.floatOrZero(m5),
          m6 = EdmCoreUtils.floatOrZero(m6)))
    }
  }
}
