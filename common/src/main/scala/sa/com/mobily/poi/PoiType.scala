/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

sealed trait PoiType { val identifier: String }

case object Home extends PoiType { override val identifier = "Home" }
case object Work extends PoiType { override val identifier = "Work" }
case object Leisure extends PoiType { override val identifier = "Leisure" }
case object HighActivity extends PoiType { override val identifier = "HighActivity" }
