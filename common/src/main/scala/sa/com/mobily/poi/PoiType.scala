/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

sealed case class PoiType(identifier: String)

object Home extends PoiType(identifier = "Home")
object Work extends PoiType(identifier = "Work")
object Other extends PoiType(identifier = "Other")
object TypeOne extends PoiType(identifier = "TypeOne")
object TypeTwo extends PoiType(identifier = "TypeTwo")
object TypeThree extends PoiType(identifier = "TypeThree")
object TypeFour extends PoiType(identifier = "TypeFour")
object TypeFive extends PoiType(identifier = "TypeFive")
object TypeSix extends PoiType(identifier = "TypeSix")
