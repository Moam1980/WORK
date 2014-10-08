/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

case class Country(countryCode: String, countryName: String)

case class MobileCountryCode(mcc: String, country: Country)

object MobileCountryCode {

  lazy val MobileCountryCodeLookup = MobileCountryCodeNameTable.stripMargin.split("\n").map { line =>
    val Array(mcc, countryCode, countryName) = line.split(":")
    (mcc, Country(countryCode.toUpperCase, countryName))
  }.toMap

  private val MobileCountryCodeNameTable =
    """412:af:Afghanistan
      |276:al:Albania
      |603:dz:Algeria
      |213:ad:Andorra
      |631:ao:Angola
      |365:ai:Anguilla
      |344:ag:Antigua & Barbuda
      |722:ar:Argentina
      |283:am:Armenia
      |363:aw:Aruba
      |505:au:Australia
      |232:at:Austria
      |400:az:Azerbaijan
      |426:bh:Bahrain
      |364:bs:Bahamas
      |470:bd:Bangladesh
      |880:bb:Barbados
      |342:bb:Barbados ??
      |257:by:Belarus
      |206:be:Belgium
      |702:bz:Belize
      |350:bm:Bermuda
      |616:bj:Benin
      |402:bt:Bhutan
      |218:ba:Bosnia and Herzegovina
      |736:bo:Bolivia
      |652:bw:Botswana
      |724:br:Brazil
      |348:vg:British Virgin Islands
      |528:bn:Brunei Darussalam
      |284:bg:Bulgaria
      |613:bf:Burkina Faso
      |642:bi:Burundi
      |456:kh:Cambodia
      |624:cm:Cameroon
      |302:ca:Canada
      |625:cv:Cape Verde
      |346:ky:Cayman Islands
      |623:cf:Central African Republic
      |622:td:Chad
      |730:cl:Chile
      |460:cn:China
      |732:co:Colombia
      |654:km:Comoros
      |629:cd:Republic of Congo
      |548:ck:Cook Islands
      |712:cr:Costa Rica
      |219:hr:Croatia
      |368:cu:Cuba
      |280:cy:Cyprus
      |230:cz:Czech Republic
      |630:cd:Democratic Republic of the Congo
      |238:dk:Denmark
      |638:dj:Djibouti
      |366:dm:Dominica
      |370:do:Dominican Republic
      |514:tl:East Timor
      |740:ec:Ecuador
      |602:eg:Egypt
      |706:sv:El Salvador
      |627:gq:Equatorial Guinea
      |248:ee:Estonia
      |636:et:Ethiopia
      |288:fo:Faroe Islands
      |542:fj:Fiji
      |244:fi:Finland
      |208:fr:France
      |547:pf:French Polynesia (France)
      |628:ga:Gabon
      |607:gm:Gambia
      |282:ge:Georgia
      |262:de:Germany
      |620:gh:Ghana
      |266:gi:Gibraltar
      |202:gr:Greece
      |290:gl:Greenland
      |352:gd:Grenada
      |340:gp:Guadeloupe
      |704:gt:Guatemala
      |611:gq:Guinea
      |632:gw:Guinea-Bissau
      |738:gy:Guyana
      |372:ht:Haiti
      |708:hn:Honduras
      |454:hk:Hong Kong
      |216:hu:Hungary
      |274:is:Iceland
      |404:in:India
      |405:in:India
      |510:id:Indonesia
      |432:ir:Iran
      |418:iq:Iraq
      |272:ie:Ireland
      |425:il:Israel
      |222:it:Italy
      |612:ci:Ivory Coast
      |338:jm:Jamaica
      |440:jp:Japan
      |416:jo:Jordan
      |401:kz:Kazakhstan
      |639:ke:Kenya
      |450:kr:Korea
      |212:xk:Kosovo
      |419:kw:Kuwait
      |437:kg:Kyrgyzstan
      |457:l:Laos
      |247:lv:Latvia
      |415:lb:Lebanon
      |651:ls:Lesotho
      |618:lr:Liberia
      |606:ly:Libya
      |295:li:Liechtenstein
      |246:lt:Lithuania
      |270:lu:Luxembourg
      |455:mo:Macao
      |294:mk:Republic of Macedonia
      |646:mg:Madagascar
      |650:mw:Malawi
      |502:my:Malaysia
      |472:mv:Maldives
      |610:ml:Mali
      |278:mt:Malta
      |609:mr:Mauritania
      |617:mu:Mauritius
      |334:mx:Mexico
      |550:fm:Micronesia
      |259:md:Moldova
      |428:mn:Mongolia
      |354:ms:Montserrat (United Kingdom)
      |604:ma:Morocco
      |643:mz:Mozambique
      |414:mm:Myanmar
      |649:na:Namibia
      |429:np:Nepal
      |204:nl:Netherlands
      |362:an:Netherlands Antilles (Netherlands)
      |546:nc:New Caledonia
      |530:nz:New Zealand
      |710:ni:Nicaragua
      |614:ne:Niger
      |621:ng:Nigeria
      |242:no:Norway
      |422:om:Oman
      |410:pk:Pakistan
      |552:pw:Palau
      |714:pa:Panama
      |537:pg:Papua New Guinea
      |744:py:Paraguay
      |716:pe:Peru
      |515:ph:Philippines
      |260:pl:Poland
      |268:pt:Portugal
      |427:qa:Qatar
      |647:re:Réunion
      |226:ro:Romania
      |250:ru:Russian Federation
      |635:rw:Rwanda
      |356:kn:Saint Kitts and Nevis
      |308:pm:Saint Pierre and Miquelon
      |360:vc:Saint Vincent and the Grenadines
      |549:ws:Samoa
      |358:lc:Saint Lucia
      |292:sm:San Marino
      |626:st:Sao Tome and Principe
      |420:sa:Saudi Arabia
      |608:sn:Senegal
      |220:cs:Serbia
      |633:sc:Seychelles
      |619:sl:Sierra Leone
      |525:sg:Singapore
      |231:sk:Slovakia
      |293:si:Slovenia
      |655:za:South Africa
      |214:es:Spain
      |413:lk:Sri Lanka
      |634:sd:Sudan
      |637:so:Somalia
      |659:ss:South Sudan
      |746:sr:Suriname
      |653:sz:Swaziland
      |240:se:Sweden
      |228:ch:Switzerland
      |417:sy:Syria
      |436:tj:Tajikistan
      |466:tw:Taiwan
      |640:tz:Tanzania
      |520:th:Thailand
      |615:tg:Togolese Republic
      |539:to:Tonga
      |374:tt:Trinidad and Tobago
      |376:tc:Turks and Caicos Islands
      |605:tn:Tunisia
      |286:tr:Turkey
      |438:tm:Turkmenistan
      |641:ug:Uganda
      |255:ua:Ukraine
      |424:ae:United Arab Emirates
      |234:gb:United Kingdom
      |235:gb:United Kingdom
      |310:us:United States of America
      |311:us:United States of America
      |316:us:United States of America
      |748:uy:Uruguay
      |434:uz:Uzbekistan
      |541:vu:Vanuatu
      |225:va:Vatican
      |734:ve:Venezuela
      |452:vn:Vietnam
      |421:ye:Yemen
      |645:zm:Zambia
      |648:zw:Zimbabwe
      |001:test:Used by GSM test equipment
      |901:int:International
      |909:*:International"""
  /**
   * Removed
   *
   * |220:cs:Montenegro
   * |222:sm:San Marino (Italy)
   * |208:mc:Monaco
   */
}
