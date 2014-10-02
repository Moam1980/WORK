/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import org.scalatest._

class EdmCoreUtilsTest extends FlatSpec with ShouldMatchers {

  trait WithManyDecimalNumbers {

    val roundDownNumber = 234929.2394829329
    val roundUpNumber = 9037832.592349201

    val roundedDownNumber = 234929.2
    val roundedUpNumber = 9037832.6
  }

  "EdmCoreUtils" should "round numbers down with one decimal" in new WithManyDecimalNumbers {
    EdmCoreUtils.roundAt1(roundDownNumber) should be (roundedDownNumber)
  }

  it should "round numbers up with one decimal" in new WithManyDecimalNumbers {
    EdmCoreUtils.roundAt1(roundUpNumber) should be (roundedUpNumber)
  }
}
