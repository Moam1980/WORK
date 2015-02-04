/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

sealed trait PoiType { val identifier: String }

case object Home extends PoiType { override val identifier = "Home" }
case object Work extends PoiType { override val identifier = "Work" }
case object TypeOne extends PoiType { override val identifier = "TypeOne" }
case object TypeTwo extends PoiType { override val identifier = "TypeTwo" }
case object TypeThree extends PoiType { override val identifier = "TypeThree" }
case object TypeFour extends PoiType { override val identifier = "TypeFour" }
case object TypeFive extends PoiType { override val identifier = "TypeFive" }
case object TypeSix extends PoiType { override val identifier = "TypeSix" }
