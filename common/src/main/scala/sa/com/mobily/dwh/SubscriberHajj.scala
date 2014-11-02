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
sealed trait NationalityGroup { val identifier: String }

case object Expat extends NationalityGroup { override val identifier = "expat" }
case object National extends NationalityGroup { override val identifier = "local" }
case object NotAvailable extends NationalityGroup { override val identifier = "n/a" }

/** Nationality group of the subscriber */
sealed trait CalculatedNationalityGroup { val identifier: String }

case object ExpatCalculated extends CalculatedNationalityGroup { override val identifier = "expat" }
case object NationalCalculated extends CalculatedNationalityGroup { override val identifier = "locals" }
case object NotAvailableCalculated extends CalculatedNationalityGroup { override val identifier = "n/a" }

case class SubscriberHajj(
    msisdn: Long,
    handsetType: String,
    handsetSubCategory: String,
    handsetVendor: String,
    smartPhone: Option[Boolean],
    nationality: String,
    segment: Segment,
    contractType: ContractType,
    pack: String,
    category: String,
    makkah7Days: Boolean,
    finalFlag: Flag,
    flagDetails: String,
    nationalityGroup: NationalityGroup,
    totalMou: Option[Long],
    destinationCountry: String,
    calculatedNationalityGroup: CalculatedNationalityGroup,
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
        handsetType = handsetTypeText,
        handsetSubCategory = EdmCoreUtils.parseNullString(handsetSubCategoryText),
        handsetVendor = EdmCoreUtils.parseNullString(handsetVendorText),
        smartPhone = EdmCoreUtils.parseYesNoBooleanOption(smartPhoneText),
        nationality = EdmCoreUtils.parseNullString(nationalityText),
        segment = parseSegment(segmentText),
        contractType = parseContractType(contractTypeText),
        pack = packText,
        category = categoryText,
        makkah7Days = EdmCoreUtils.parseYesNoBoolean(makkah7DaysText),
        finalFlag = parseFlag(finalFlagText),
        flagDetails = flagDetailsText,
        nationalityGroup = parseNationalityGroup(nationalityGroupText),
        totalMou = EdmCoreUtils.parseLong(totalMouText),
        destinationCountry = EdmCoreUtils.parseNullString(destinationCountryText),
        calculatedNationalityGroup = parseCalculatedNationalityGroup(calculatedNationalityGroupText),
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
    case Expat.identifier => Expat
    case National.identifier => National
    case NotAvailable.identifier => NotAvailable
  }

  def parseCalculatedNationalityGroup(calculatedNationalityGroupText: String): CalculatedNationalityGroup =
    calculatedNationalityGroupText.toLowerCase match {
    case ExpatCalculated.identifier => ExpatCalculated
    case NationalCalculated.identifier => NationalCalculated
    case NotAvailableCalculated.identifier => NotAvailableCalculated
  }
}
