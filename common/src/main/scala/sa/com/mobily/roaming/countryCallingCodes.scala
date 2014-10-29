/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

case class CountryCallingCode(callingCode: Int, countryName: String)

object CountryCallingCode {

  lazy val CountryCallingCodeLookup = countryCallingCodeNameTable.stripMargin.split("\n").map { line =>
    val Array(countryCallingCode, countryName) = line.split(":")
    (countryCallingCode.toInt, countryName)
  }.toMap

  lazy val maxLengthCountryCallingCode = CountryCallingCodeLookup.keys.max.toString.length

  private val countryCallingCodeNameTable =
    """20:Egypt
      |27:South Africa
      |30:Greece
      |31:Netherlands
      |32:Belgium
      |33:France
      |34:Spain
      |36:Hungary
      |39:Italy
      |40:Romania
      |41:Switzerland
      |43:Austria
      |44:United Kingdom
      |45:Denmark
      |46:Sweden
      |47:Norway
      |48:Poland
      |49:Germany
      |51:Peru
      |52:Mexico
      |53:Cuba
      |54:Argentina
      |55:Brazil
      |56:Chile
      |57:Colombia
      |58:Venezuela
      |60:Malaysia
      |61:Australia
      |62:Indonesia
      |63:Philippines
      |64:New Zealand
      |65:Singapore
      |66:Thailand
      |77:Kazakhstan
      |78:Russia
      |79:Russia
      |81:Japan
      |82:Republic of Korea
      |84:Vietnam
      |86:China
      |90:Turkey
      |91:India
      |92:Pakistan
      |93:Afghanistan
      |94:Sri Lanka
      |95:Myanmar
      |98:Iran
      |211:South Sudan
      |212:Morocco
      |213:Algeria
      |216:Tunisia
      |218:Libya
      |220:Gambia
      |221:Senegal
      |222:Mauritania
      |223:Mali
      |224:Guinea
      |225:Ivory Coast
      |226:Burkina Faso
      |227:Niger
      |228:Togo
      |229:Benin
      |230:Mauritius
      |231:Liberia
      |232:Sierra Leone
      |233:Ghana
      |234:Nigeria
      |235:Chad
      |236:Central African Republic
      |237:Cameroon
      |238:Cape Verde
      |239:Sao Tome
      |240:Equitorial Guinea
      |241:Gabon Republic
      |242:Republic of the Congo
      |243:Democratic Republic of the Congo
      |244:Angola
      |245:Guinea-Bissau
      |248:Seychelles
      |249:Sudan
      |250:Rwanda
      |251:Ethiopia
      |252:Somalia Republic
      |253:Djibouti
      |254:Kenya
      |255:United Republic of Tanzania
      |256:Uganda
      |257:Burundi
      |258:Mozambique
      |260:Zambia
      |261:Madagascar
      |262:Reunion Island
      |263:Zimbabwe
      |264:Namibia
      |265:Malawi
      |266:Lesotho
      |267:Botswana
      |268:Swaziland
      |290:Saint Helena
      |291:Eritrea
      |297:Aruba
      |298:Faroe Islands
      |299:Greenland
      |350:Gibraltar
      |351:Portugal
      |352:Luxembourg
      |353:Ireland
      |354:Iceland
      |355:Albania
      |356:Malta
      |357:Cyprus
      |358:Finland
      |359:Bulgaria
      |370:Lithuania
      |371:Latvia
      |372:Estonia
      |373:Republic of Moldova
      |374:Armenia
      |375:Belarus
      |376:Andorra
      |377:Monaco
      |378:San Marino
      |380:Ukraine
      |381:Yugoslavia
      |382:Montenegro
      |385:Croatia
      |386:Slovenia
      |387:Bosnia and Herzegovina
      |389:The former Yugoslav Republic of Macedonia
      |420:Czech Republic
      |421:Slovakia
      |423:Liechtenstein
      |500:Falkland Islands
      |501:Belize
      |502:Guatemala
      |503:El Salvador
      |504:Honduras
      |505:Nicaragua
      |506:Costa Rica
      |507:Panama
      |508:Saint Pierre and Miquelon
      |509:Haiti
      |590:Saint Barthelemy
      |591:Bolivia
      |592:Guyana
      |593:Ecuador
      |595:Paraguay
      |596:Martinique
      |597:Suriname
      |598:Uruguay
      |599:Netherlands Antilles
      |670:Timor-Leste
      |672:Antarctica
      |673:Brunei Darussalam
      |674:Nauru
      |675:Papua New Guinea
      |676:Tonga
      |677:Solomon Islands
      |678:Vanuatu
      |679:Fiji
      |680:Palau
      |681:Wallisand Futuna Islands
      |682:Cook Islands
      |683:Niue
      |684:American Samoa
      |685:Samoa
      |686:Kiribati
      |687:New Caledonia
      |688:Tuvalu
      |689:French Polynesia
      |690:Tokelau
      |691:Micronesia
      |692:Marshall Islands
      |850:North Korea
      |852:Hong Kong
      |853:Macao Special Administrative Region of China
      |855:Cambodia
      |856:Lao People's Democratic Republic
      |870:Pitcairn Islands
      |880:Bangladesh
      |886:Taiwan
      |960:Maldives
      |961:Lebanon
      |962:Jordan
      |963:Syria
      |964:Iraq
      |965:Kuwait
      |966:Saudi Arabia
      |967:Yemen
      |968:Oman
      |970:Occupied Palestinian Territory
      |971:United Arab Emirates
      |972:Palestine
      |973:Bahrain
      |974:Qatar
      |975:Bhutan
      |976:Mongolia
      |977:Nepal
      |992:Tajikistan
      |993:Turkmenistan
      |994:Azerbaijan
      |995:Georgia
      |996:Kyrgyzstan
      |998:Uzbekistan
      |1201:United States
      |1202:United States
      |1203:United States
      |1204:Canada
      |1205:United States
      |1206:United States
      |1207:United States
      |1208:United States
      |1209:United States
      |1210:United States
      |1212:United States
      |1213:United States
      |1214:United States
      |1215:United States
      |1216:United States
      |1217:United States
      |1218:United States
      |1219:United States
      |1224:United States
      |1225:United States
      |1226:Canada
      |1228:United States
      |1229:United States
      |1231:United States
      |1234:United States
      |1239:United States
      |1240:United States
      |1242:Bahamas
      |1246:Barbados
      |1248:United States
      |1249:Canada
      |1250:Canada
      |1251:United States
      |1252:United States
      |1253:United States
      |1254:United States
      |1256:United States
      |1260:United States
      |1262:United States
      |1264:Anguilla
      |1267:United States
      |1268:Antigua and Barbuda
      |1269:United States
      |1270:United States
      |1276:United States
      |1281:United States
      |1284:British Virgin Islands
      |1289:Canada
      |1301:United States
      |1302:United States
      |1303:United States
      |1304:United States
      |1305:United States
      |1306:Canada
      |1307:United States
      |1308:United States
      |1309:United States
      |1310:United States
      |1312:United States
      |1313:United States
      |1314:United States
      |1315:United States
      |1316:United States
      |1317:United States
      |1318:United States
      |1319:United States
      |1320:United States
      |1321:United States
      |1323:United States
      |1325:United States
      |1330:United States
      |1331:United States
      |1334:United States
      |1336:United States
      |1337:United States
      |1339:United States
      |1340:US Virgin Islands
      |1343:Canada
      |1345:Cayman Islands
      |1347:United States
      |1351:United States
      |1352:United States
      |1360:United States
      |1361:United States
      |1385:United States
      |1386:United States
      |1401:United States
      |1402:United States
      |1403:Canada
      |1404:United States
      |1405:United States
      |1406:United States
      |1407:United States
      |1408:United States
      |1409:United States
      |1410:United States
      |1412:United States
      |1413:United States
      |1414:United States
      |1415:United States
      |1416:Canada
      |1417:United States
      |1418:Canada
      |1419:United States
      |1423:United States
      |1424:United States
      |1425:United States
      |1430:United States
      |1432:United States
      |1434:United States
      |1435:United States
      |1438:Canada
      |1440:United States
      |1441:Bermuda
      |1442:United States
      |1443:United States
      |1450:Canada
      |1458:United States
      |1469:United States
      |1470:United States
      |1473:Grenada and Carriacuou
      |1475:United States
      |1478:United States
      |1479:United States
      |1480:United States
      |1484:United States
      |1501:United States
      |1502:United States
      |1503:United States
      |1504:United States
      |1505:United States
      |1506:Canada
      |1507:United States
      |1508:United States
      |1509:United States
      |1510:United States
      |1512:United States
      |1513:United States
      |1514:Canada
      |1515:United States
      |1516:United States
      |1517:United States
      |1518:United States
      |1519:Canada
      |1520:United States
      |1530:United States
      |1531:United States
      |1534:United States
      |1539:United States
      |1540:United States
      |1541:United States
      |1551:United States
      |1559:United States
      |1561:United States
      |1562:United States
      |1563:United States
      |1567:United States
      |1570:United States
      |1571:United States
      |1573:United States
      |1574:United States
      |1575:United States
      |1579:Canada
      |1580:United States
      |1581:Canada
      |1585:United States
      |1586:United States
      |1587:Canada
      |1599:Saint Martin
      |1601:United States
      |1602:United States
      |1603:United States
      |1604:Canada
      |1605:United States
      |1606:United States
      |1607:United States
      |1608:United States
      |1609:United States
      |1610:United States
      |1612:United States
      |1613:Canada
      |1614:United States
      |1615:United States
      |1616:United States
      |1617:United States
      |1618:United States
      |1619:United States
      |1620:United States
      |1623:United States
      |1626:United States
      |1630:United States
      |1631:United States
      |1636:United States
      |1641:United States
      |1646:United States
      |1647:Canada
      |1649:Turks and Caicos Islands
      |1650:United States
      |1651:United States
      |1657:United States
      |1660:United States
      |1661:United States
      |1662:United States
      |1664:Montserrat
      |1670:Northern Mariana Islands
      |1671:Guam
      |1678:United States
      |1682:United States
      |1701:United States
      |1702:United States
      |1703:United States
      |1704:United States
      |1705:Canada
      |1706:United States
      |1707:United States
      |1708:United States
      |1709:Canada
      |1712:United States
      |1713:United States
      |1714:United States
      |1715:United States
      |1716:United States
      |1717:United States
      |1718:United States
      |1719:United States
      |1720:United States
      |1724:United States
      |1727:United States
      |1731:United States
      |1732:United States
      |1734:United States
      |1740:United States
      |1754:United States
      |1757:United States
      |1758:Saint Lucia
      |1760:United States
      |1762:United States
      |1763:United States
      |1765:United States
      |1767:Dominica
      |1769:United States
      |1770:United States
      |1772:United States
      |1773:United States
      |1774:United States
      |1775:United States
      |1778:Canada
      |1780:Canada
      |1781:United States
      |1784:Saint Vincent and the Grenadines
      |1785:United States
      |1786:United States
      |1787:Puerto Rico
      |1801:United States
      |1802:United States
      |1803:United States
      |1804:United States
      |1805:United States
      |1806:United States
      |1807:Canada
      |1808:United States
      |1809:Dominican Republic
      |1810:United States
      |1812:United States
      |1813:United States
      |1814:United States
      |1815:United States
      |1816:United States
      |1817:United States
      |1818:United States
      |1819:Canada
      |1828:United States
      |1830:United States
      |1831:United States
      |1832:United States
      |1843:United States
      |1845:United States
      |1847:United States
      |1848:United States
      |1850:United States
      |1856:United States
      |1857:United States
      |1858:United States
      |1859:United States
      |1860:United States
      |1862:United States
      |1863:United States
      |1864:United States
      |1865:United States
      |1866:Call to satellite
      |1867:Canada
      |1868:Trinidad and Tobago
      |1869:Saint Kitts and Nevis
      |1870:United States
      |1872:United States
      |1876:Jamaica
      |1878:United States
      |1901:United States
      |1902:Canada
      |1903:United States
      |1904:United States
      |1905:Canada
      |1906:United States
      |1907:United States
      |1908:United States
      |1909:United States
      |1910:United States
      |1912:United States
      |1913:United States
      |1914:United States
      |1915:United States
      |1916:United States
      |1917:United States
      |1918:United States
      |1919:United States
      |1920:United States
      |1925:United States
      |1928:United States
      |1929:United States
      |1931:United States
      |1936:United States
      |1937:United States
      |1938:United States
      |1940:United States
      |1941:United States
      |1947:United States
      |1949:United States
      |1951:United States
      |1952:United States
      |1954:United States
      |1956:United States
      |1970:United States
      |1971:United States
      |1972:United States
      |1973:United States
      |1978:United States
      |1979:United States
      |1980:United States
      |1985:United States
      |1989:United States
      |2697:Comoros
      |7310:Kazakhstan
      |7313:Kazakhstan
      |7314:Kazakhstan
      |7315:Kazakhstan
      |7316:Kazakhstan
      |7317:Kazakhstan
      |7318:Kazakhstan
      |7321:Kazakhstan
      |7322:Kazakhstan
      |7323:Kazakhstan
      |7324:Kazakhstan
      |7325:Kazakhstan
      |7326:Kazakhstan
      |7327:Kazakhstan
      |7329:Kazakhstan
      |7330:Kazakhstan
      |7336:Kazakhstan
      |8816:Iridium
      |8817:Iridium
      |8818:Globalstar
      |8819:Globalstar
      |8821:Call to satellite
      |88216:Thuraya
      |441624:Isle of Man"""
}
