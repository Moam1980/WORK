/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm

import scala.language.existentials
import scala.util.Try

import org.apache.spark.sql._
import org.joda.time.format.DateTimeFormat

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

/** Types of customer identifications*/
sealed case class CustomerIdType(id: String)

object Visa extends CustomerIdType(id = "Visa")
object Passport extends CustomerIdType(id = "Passport Number")
object Ncbs extends CustomerIdType(id = "NCBS ID")
object Invalid extends CustomerIdType(id = "Invalid ID type")
object SaudiNational extends CustomerIdType(id = "Saudi National ID")
object Gcc extends CustomerIdType(id = "GCC ID")
object DiplomaticCard extends CustomerIdType(id = "Diplomatic Card")
object TaxNumber extends CustomerIdType(id = "Tax Identification Number")
object MobilyEmployee extends CustomerIdType(id = "Mobily Employee ID")
object CommercialRegistration extends CustomerIdType(id = "Commercial Registration ID")
object MobilyCostCenter extends CustomerIdType(id = "Mobily Cost Center")
object Iqama extends CustomerIdType(id = "IQAMA")
object FamilyCard extends CustomerIdType(id = "Family Card")
object Ms extends CustomerIdType(id = "MS")
object DriverLicenceNumber extends CustomerIdType(id = "Driver's Licence Number")
object UnknownIdType extends CustomerIdType (id = "Unknown")

/** Types of customer payment*/
sealed case class PayType(id: String)

object PrePaid extends PayType(id = "Pre-Paid")
object PostPaid extends PayType(id = "Post-Paid")
object UnknownPayType extends PayType (id = "Unknown")

sealed case class DataPackage(id: String)

object Data extends DataPackage(id = "Data")
object Voice extends DataPackage(id = "Voice")
object UnknownDataPackage extends DataPackage (id = "Unknown")

sealed case class CorpPackage(id: String)

object Iuc extends CorpPackage(id = "IUC")
object LargeCorporate extends CorpPackage(id = "Large Corporate")
object RetailCustomer extends CorpPackage(id = "Retail Customer")
object UnknownCorpPackage extends CorpPackage (id = "Unknown")

sealed case class ActiveStatus(id: String)

object Ots extends ActiveStatus(id = "OTS")
object Active extends ActiveStatus(id = "Active")
object HotSIM extends ActiveStatus(id = "HotSIM")
object UnknownActiveStatus extends ActiveStatus (id = "Unknown")

sealed case class SourceActivation(id: String)

object Mcr extends SourceActivation(id = "MCR")
object Siebel extends SourceActivation(id = "Siebel")
object Mdm extends SourceActivation(id = "MDM")
object UnknownSourceActivation extends SourceActivation (id = "Unknown")

sealed case class CalculatedSegment(id: String)

object Wcs extends CalculatedSegment(id = "W")
object S50 extends CalculatedSegment(id = "S50")
object S40 extends CalculatedSegment(id = "S40")
object S60 extends CalculatedSegment(id = "S60")
object S80 extends CalculatedSegment(id = "S80")
object S90 extends CalculatedSegment(id = "S90")
object UnknownSourceCalculatedSegment extends CalculatedSegment (id = "Unknown")

case class Revenues (
    m1: Float,
    m2: Float,
    m3: Float,
    m4: Float,
    m5: Float,
    m6: Float) {

  lazy val totalRevenue = m1 + m2 + m3 + m4 + m5 + m6
}

case class SubscriberDates(
    activation: Option[Long],
    lastActivity: Option[Long],
    lastRecharge: Option[Long])

case class SubscriberPackages(
    data: DataPackage,
    corp: CorpPackage)

case class SubscriberTypes(
    pay: PayType,
    handset: String)

case class Nationalities(declared: String, inferred:String)

case class Subscriber(
    user: User,
    idType: CustomerIdType,
    idNumber: Option[Long],
    age: Option[Float],
    gender: String,
    siteId: Option[Int],
    regionId: Option[Short],
    nationalities: Nationalities,
    types: SubscriberTypes,
    packages: SubscriberPackages,
    date: SubscriberDates,
    activeStatus: ActiveStatus,
    sourceActivation: SourceActivation,
    roamingStatus: String,
    currentBalance: Option[Float],
    calculatedSegment: CalculatedSegment,
    revenues: Revenues)

object Subscriber {

  val inputDateTimeFormat = "MM/dd/yyyy"
  final val fmt = DateTimeFormat.forPattern(inputDateTimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)

  implicit val fromCsv = new CsvParser[Subscriber] {

    final lazy val nationalitiesToNormalize: Map[String, String] = Map(("ANTIGUA&BARBUD", "ANTIGUA AND BARBUDA"),
      ("BGD", "BANGLADESH"), ("BOSNIA AND HERZEGOWINA", "BOSNIA HERZEGOVINA"),
      ("BOSNIA AND HERZEGOVINA", "BOSNIA HERZEGOVINA"), ("BOSNIA&HERZEG", "BOSNIA HERZEGOVINA"),
      ("CHAD REPUBLIC", "CHAD"), ("DEMOCRATIC REPUBLIC OF THE CONGO", "CONGO"), ("REPUBLIC OF THE CONGO", "CONGO"),
      ("EQUITORIAL GUINEA", "EQUATORIAL GUINEA"), ("ETH", "ETHIOPIA"), ("EGY", "EGYPT"), ("EYGEPT", "EGYPT"),
      ("FALKLAND ISLAN", "FALKLAND ISLANDS"), ("GABON REPUBLIC", "GABON"), ("HOLY SEE (VATICAN CITY STATE", "HOLY SEE"),
      ("IDN", "INDONESIA"), ("IND", "INDIA"), ("IRAN (ISLAMIC REPUBLIC OF", "IRAN"), ("KSA", "SAUDI ARABIA"),
      ("SAUDI", "SAUDI ARABIA"), ("SAU", "SAUDI ARABIA"), ("MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF", "MACEDONIA"),
      ("MACEDONIA, THE FORMER YUGOSLAV", "MACEDONIA"), ("MACETHE FORMER YUGOSLAV REPUBLIC OF MACEDONIA", "MACEDONIA"),
      ("MOLDOVA, REPUBLIC OF", "MOLDOVA"), ("MARSHAL ISLAND", "MARSHALL ISLANDS"), ("RUSSIAN FEDERATION", "RUSSIA"),
      ("SLOVAKIA (SLOVAK REPUBLIC", "SLOVAKIA"), ("SOMALIA REPUBLIC", "SOMALIA"), ("SYRIAN ARAB REPUBLIC", "SYRIA"),
      ("TAIWAN, PROVINCE OF CHINA", "TAIWAN"), ("TANZANIA, UNITED REPUBLIC OF", "TANZANIA"),
      ("TRINIDAD&TOBAG", "TRINIDAD AND TOBAGO"), ("TUR", "TURKEY"), ("UNITED ARABEMIRATES", "UAE"),
      ("UNITED ARAB EMIRATES", "UAE"), ("UNITED STATES OF AMERICA", "USA"), ("UNITED STATES", "USA"),
      ("VIET NAM", "VIETNAM"), ("0", ""), ("999", ""), ("999 FROM ALELM", ""), ("????", ""), ("?????", ""),
      ("???????", ""), ("????????", ""), ("����", ""), ("�������", ""), ("��������", ""))

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): Subscriber = {
      val (firstChunk, revenueChunk) = fields.splitAt(22)  // scalastyle:ignore magic.number
      val Array(msisdnText, idTypeText, idNumberText, imeiText, imsiText, genderText, siteIdText, regionIdText,
        actualNationalityText, calNationalityText, payTypeText, isDataPackageText, ageText, isCorpPackageText,
        activationDateText, activeStatusText, sourceActivationText, roamingStatusText, handsetTypeText,
        currentBalanceText, lastActivityDateText, lastRechargeDateText) = firstChunk
      val Array(m1CalculatedSegmentText, m1RevenueText, m2RevenueText, m3RevenueText, m4RevenueText, m5RevenueText,
        m6RevenueText) = revenueChunk

      Subscriber(
        user = User(imeiText, imsiText, EdmCoreUtils.parseLong(msisdnText).getOrElse(0L)),
        idType = parseCustomerIdType(idTypeText),
        idNumber = EdmCoreUtils.parseLong(idNumberText),
        age = EdmCoreUtils.parseFloat(ageText),
        gender = genderText,
        siteId = EdmCoreUtils.parseInt(siteIdText),
        regionId = EdmCoreUtils.parseShort(regionIdText),
        nationalities = Nationalities(
          nationalitiesToNormalize.getOrElse(actualNationalityText.toUpperCase, actualNationalityText.toUpperCase),
          nationalitiesToNormalize.getOrElse(calNationalityText.toUpperCase, calNationalityText.toUpperCase)),
        types = SubscriberTypes(parsePayType(payTypeText), handsetTypeText),
        packages = SubscriberPackages(parseDataPackage(isDataPackageText), parseCorpPackage(isCorpPackageText)),
        date = SubscriberDates(
          activation = parseDate(activationDateText),
          lastActivity = parseDate(lastActivityDateText),
          lastRecharge = parseDate(lastRechargeDateText)),
        activeStatus = parseActiveStatus(activeStatusText),
        sourceActivation = parseSourceActivation(sourceActivationText),
        roamingStatus = roamingStatusText,
        currentBalance = EdmCoreUtils.parseFloat(currentBalanceText),
        calculatedSegment = parseCalculatedSegment(m1CalculatedSegmentText),
        revenues = Revenues(
          m1= m1RevenueText.toFloat,
          m2 = m2RevenueText.toFloat,
          m3 = m3RevenueText.toFloat,
          m4 = m4RevenueText.toFloat,
          m5 = m5RevenueText.toFloat,
          m6 = m6RevenueText.toFloat))
    }
  }

  implicit val fromRow = new RowParser[Subscriber] {

    override def fromRow(row: Row): Subscriber = {// scalastyle:ignore method.length
      val Seq(
        userRow,
        idTypeRow,
        idNumber,
        age,
        gender,
        siteId,
        regionId,
        nationalitiesRow,
        typesRow,
        packagesRow,
        dateRow,
        activeStatusRow,
        sourceActivationRow,
        roamingStatus,
        currentBalance,
        calculatedSegmentRow,
        revenuesRow) = row.toSeq
      val user =  User.fromRow.fromRow(userRow.asInstanceOf[Row])
      val Seq(customerIdType) = idTypeRow.asInstanceOf[Row].toSeq
      val Seq(declared, inferred) = nationalitiesRow.asInstanceOf[Row].toSeq
      val Seq(payRow, handset) = typesRow.asInstanceOf[Row].toSeq
      val Seq(pay) = payRow.asInstanceOf[Row].toSeq
      val Seq(Seq(data), Seq(corp)) = packagesRow.asInstanceOf[Row].toSeq
      val Seq(activation, lastActivity, lastRecharge) = dateRow.asInstanceOf[Row].toSeq
      val Seq(activeStatus) = activeStatusRow.asInstanceOf[Row].toSeq
      val Seq(sourceActivation) = sourceActivationRow.asInstanceOf[Row].toSeq
      val Seq(calculatedSegment) = calculatedSegmentRow.asInstanceOf[Row].toSeq
      val Seq(m1, m2, m3 , m4, m5, m6) = revenuesRow.asInstanceOf[Row].toSeq

      Subscriber(
        user = user,
        idType = CustomerIdType(customerIdType.asInstanceOf[String]),
        idNumber = EdmCoreUtils.longOption(idNumber),
        age = EdmCoreUtils.floatOption(age),
        gender = gender.asInstanceOf[String],
        siteId = EdmCoreUtils.intOption(siteId),
        regionId = EdmCoreUtils.shortOption(regionId),
        nationalities = Nationalities(declared.asInstanceOf[String], inferred.asInstanceOf[String]),
        types = SubscriberTypes(PayType(pay.asInstanceOf[String]), handset.asInstanceOf[String]),
        packages = SubscriberPackages(DataPackage(data.asInstanceOf[String]), CorpPackage(corp.asInstanceOf[String])),
        date = SubscriberDates(
          activation = EdmCoreUtils.longOption(activation),
          lastActivity = EdmCoreUtils.longOption(lastActivity),
          lastRecharge = EdmCoreUtils.longOption(lastRecharge)),
        activeStatus = ActiveStatus(activeStatus.asInstanceOf[String]),
        sourceActivation = SourceActivation(sourceActivation.asInstanceOf[String]),
        roamingStatus = roamingStatus.asInstanceOf[String],
        currentBalance = EdmCoreUtils.floatOption(currentBalance),
        calculatedSegment = CalculatedSegment(calculatedSegment.asInstanceOf[String]),
        revenues = Revenues(
          m1 = m1.asInstanceOf[Float],
          m2 = m2.asInstanceOf[Float],
          m3 = m3.asInstanceOf[Float],
          m4 = m4.asInstanceOf[Float],
          m5 = m5.asInstanceOf[Float],
          m6 = m6.asInstanceOf[Float]))
    }
  }

  def parseCustomerIdType (identifier: String): CustomerIdType = {//scalastyle:ignore cyclomatic.complexity
    identifier match {
      case Visa.id => Visa
      case Passport.id => Passport
      case Ncbs.id => Ncbs
      case Invalid.id => Invalid
      case SaudiNational.id => SaudiNational
      case Gcc.id => Gcc
      case DiplomaticCard.id => DiplomaticCard
      case TaxNumber.id => TaxNumber
      case MobilyEmployee.id => MobilyEmployee
      case CommercialRegistration.id => CommercialRegistration
      case MobilyCostCenter.id => MobilyCostCenter
      case Iqama.id => Iqama
      case FamilyCard.id => FamilyCard
      case Ms.id => Ms
      case DriverLicenceNumber.id => DriverLicenceNumber
      case _ =>  UnknownIdType
    }
  }

  def parsePayType (payType: String): PayType = payType match {
    case PrePaid.id => PrePaid
    case PostPaid.id => PostPaid
    case _ => UnknownPayType
  }

  def parseDataPackage (dataPackage: String): DataPackage = dataPackage match{
    case Voice.id => Voice
    case Data.id => Data
    case _ => UnknownDataPackage
  }

  def parseCorpPackage (corpPackage: String): CorpPackage = corpPackage match {
    case Iuc.id => Iuc
    case LargeCorporate.id => LargeCorporate
    case RetailCustomer.id => RetailCustomer
    case _ => UnknownCorpPackage
  }

  def parseActiveStatus (status: String): ActiveStatus = status match {
    case Ots.id => Ots
    case Active.id => Active
    case HotSIM.id => HotSIM
    case _ => UnknownActiveStatus
  }

  def parseSourceActivation (source: String): SourceActivation = source match {
    case Mcr.id => Mcr
    case Siebel.id => Siebel
    case Mdm.id => Mdm
    case _ => UnknownSourceActivation
  }

  def parseCalculatedSegment (segment: String): CalculatedSegment = segment match {
    case S60.id => S60
    case Wcs.id => Wcs
    case S50.id => S50
    case S40.id => S40
    case S90.id => S90
    case S80.id => S80
    case _ => UnknownSourceCalculatedSegment
  }

  def parseDate(s: String): Option[Long] = Try { fmt.parseDateTime(s).getMillis }.toOption
}
// scalastyle:ignore number.of.types
