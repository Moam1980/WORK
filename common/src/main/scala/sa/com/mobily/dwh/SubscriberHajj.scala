/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.utils.EdmCoreUtils

/** Segment of the subscriber */
sealed trait Segment { val identifier: String }

case object ConsumerSegment extends Segment { override val identifier = "consumer" }
case object BusinessSegment extends Segment { override val identifier = "business" }
case object IgnoreSegment extends Segment { override val identifier = "ignore" }
case object TestSegment extends Segment { override val identifier = "test" }

/** Contract type of the subscriber */
sealed trait ContractType { val identifier: String }

case object PrepaidContract extends ContractType { override val identifier = "prepaid" }
case object PostpaidContract extends ContractType { override val identifier = "postpaid" }
case object BusinessContract extends ContractType { override val identifier = "business" }
case object BBContract extends ContractType { override val identifier = "bb" }
case object IgnoreContract extends ContractType { override val identifier = "ignore" }
case object TestContract extends ContractType { override val identifier = "test" }

/** Flag of the subscriber */
sealed trait Flag { val identifier: String }

case object InternalFlag extends Flag { override val identifier = "internal" }
case object InternalVisitorFlag extends Flag { override val identifier = "internal_visitor" }
case object ExternalFlag extends Flag { override val identifier = "external" }
case object LocalFlag extends Flag { override val identifier = "local" }
case object DefaultFlag extends Flag { override val identifier = "default" }
case object IgnoreFlag extends Flag { override val identifier = "ignore" }

/** Nationality group of the subscriber */
sealed trait NationalityGroup { val identifiers: List[String] }

case object Expat extends NationalityGroup { override val identifiers = List("expat") }
case object National extends NationalityGroup { override val identifiers = List("local", "locals") }
case object NotAvailable extends NationalityGroup { override val identifiers = List("n/a") }

case class SubscriberHierarchy(
    finalFlag: Flag,
    segment: Segment,
    contractType: ContractType,
    category: String,
    pack: String,
    nationalityGroup: NationalityGroup,
    nationality: String)

case class Handset(
    handsetType: String,
    handsetSubCategory: String,
    handsetVendor: String,
    smartPhone: Option[Boolean])

case class SubscriberHajj(
    msisdn: Long,
    handset: Handset,
    hierarchy: SubscriberHierarchy,
    makkah7Days: Boolean,
    flagDetails: String,
    totalMou: Option[Long],
    destinationCountry: String,
    calculatedNationalityGroup: NationalityGroup,
    numberRecharges: Option[Float],
    rechargeAmount: Option[Float],
    subsciberRen: Boolean,
    subsciberRenRev: Option[Float])

object SubscriberHajj {

  final val lineCsvParserObject = new OpenCsvParser(separator = '\t')

  implicit val fromCsv = new CsvParser[SubscriberHajj] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberHajj = {
      val Array(msisdnText, handsetTypeText, handsetSubCategoryText, handsetVendorText, smartPhoneText,
        nationalityText, segmentText, contractTypeText, packText, categoryText, makkah7DaysText, finalFlagText,
        flagDetailsText, nationalityGroupText, totalMouText, destinationCountryText,
        calculatedNationalityGroupText, numberRechargesText, rechargeAmountText, subsciberRenText,
        subsciberRenRevText) = fields

      SubscriberHajj(
        msisdn = msisdnText.toLong,
        handset = Handset(
          handsetType = handsetTypeText,
          handsetSubCategory = EdmCoreUtils.parseNullString(handsetSubCategoryText),
          handsetVendor = EdmCoreUtils.parseNullString(handsetVendorText),
          smartPhone = EdmCoreUtils.parseYesNoBooleanOption(smartPhoneText)),
        hierarchy = SubscriberHierarchy(
          finalFlag = parseFlag(finalFlagText),
          segment = parseSegment(segmentText),
          contractType = parseContractType(contractTypeText),
          category = categoryText,
          pack = packText,
          nationalityGroup = parseNationalityGroup(nationalityGroupText),
          nationality = EdmCoreUtils.parseNullString(nationalityText)),
        makkah7Days = EdmCoreUtils.parseYesNoBoolean(makkah7DaysText),
        flagDetails = flagDetailsText,
        totalMou = EdmCoreUtils.parseLong(totalMouText),
        destinationCountry = EdmCoreUtils.parseNullString(destinationCountryText),
        calculatedNationalityGroup = parseNationalityGroup(calculatedNationalityGroupText),
        numberRecharges = EdmCoreUtils.parseFloat(numberRechargesText),
        rechargeAmount = EdmCoreUtils.parseFloat(rechargeAmountText),
        subsciberRen = EdmCoreUtils.parseYesNoBoolean(subsciberRenText),
        subsciberRenRev = EdmCoreUtils.parseFloat(subsciberRenRevText))
    }
  }

  def parseSegment(segmentText: String): Segment = segmentText.toLowerCase match {
    case ConsumerSegment.identifier => ConsumerSegment
    case BusinessSegment.identifier => BusinessSegment
    case IgnoreSegment.identifier => IgnoreSegment
    case TestSegment.identifier => TestSegment
  }

  def parseContractType(contractTypeText: String): ContractType = contractTypeText.toLowerCase match {
    case PrepaidContract.identifier => PrepaidContract
    case PostpaidContract.identifier => PostpaidContract
    case BusinessContract.identifier => BusinessContract
    case BBContract.identifier => BBContract
    case IgnoreContract.identifier => IgnoreContract
    case TestContract.identifier => TestContract
  }

  def parseFlag(flagText: String): Flag = flagText.toLowerCase match {
    case InternalFlag.identifier => InternalFlag
    case InternalVisitorFlag.identifier => InternalVisitorFlag
    case ExternalFlag.identifier => ExternalFlag
    case LocalFlag.identifier => LocalFlag
    case DefaultFlag.identifier => DefaultFlag
    case IgnoreFlag.identifier => IgnoreFlag
  }

  def parseNationalityGroup(nationalityGroupText: String): NationalityGroup = nationalityGroupText.toLowerCase match {
    case value if Expat.identifiers.contains(value) => Expat
    case value if National.identifiers.contains(value) => National
    case value if NotAvailable.identifiers.contains(value)  => NotAvailable
  }
}
