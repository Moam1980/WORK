/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import sa.com.mobily.parsing.OpenCsvParser

case class Country(
    mcc: String,
    callingCode: String,
    isoCode: String,
    name: String)

case class CountryOperator(
    mnc: String,
    operator: String,
    country: Country)

object CountryCode {

  final val LineCsvParserObject = new OpenCsvParser(separator = ',')

  val SaudiArabiaIsoCode = "sa"

  lazy val MccCountryLookup = CountryTable.map { countryOperator =>
    (countryOperator.country.mcc, countryOperator.country)
  }.toMap

  lazy val MccMncCountryLookup = CountryTable.map { countryOperator =>
    (countryOperator.country.mcc + countryOperator.mnc, countryOperator.country)
  }.toMap

  lazy val MccCountryOperatorsLookup = CountryTable.groupBy(_.country.mcc)

  lazy val CallingCodeCountryLookup = CountryTable.map { countryOperator =>
    (countryOperator.country.callingCode, countryOperator.country)
  }.toMap

  lazy val CountryTable = CountryTable1 ::: CountryTable2

  lazy val CountryTable1 = countryTable1.stripMargin.split("\n").map { line => parseLine(line) }.toList

  lazy val CountryTable2 = countryTable2.stripMargin.split("\n").map { line => parseLine(line) }.toList

  private def parseLine(line: String): CountryOperator = {
    val Array(mccText, callingCodeText, isoCodeText, nameText, mncText, operatorText) =
      LineCsvParserObject.parseLine(line)
    val country = Country(
      mcc = mccText,
      callingCode = callingCodeText,
      isoCode = isoCodeText.toUpperCase,
      name = nameText)

    CountryOperator(
      mnc = mncText,
      operator = operatorText,
      country = country)
  }

  private val countryTable1: String =
    """289,7,ab,Abkhazia,88,A-Mobile
      |289,7,ab,Abkhazia,68,A-Mobile
      |289,7,ab,Abkhazia,67,Aquafon
      |412,93,af,Afghanistan,88,Afghan Telecom Corp. (AT)
      |412,93,af,Afghanistan,80,Afghan Telecom Corp. (AT)
      |412,93,af,Afghanistan,01,Afghan Wireless/AWCC
      |412,93,af,Afghanistan,40,Areeba
      |412,93,af,Afghanistan,50,Etisalat
      |412,93,af,Afghanistan,20,Roshan
      |276,355,al,Albania,01,AMC Mobil
      |276,355,al,Albania,03,Eagle Mobile
      |276,355,al,Albania,04,PLUS Communication Sh.a
      |276,355,al,Albania,02,Vodafone
      |603,213,dz,Algeria,01,ATM Mobils
      |603,213,dz,Algeria,02,Orascom / DJEZZY
      |603,213,dz,Algeria,03,Wataniya / Nedjma
      |544,684,as,American Samoa,11,Blue Sky Communications
      |213,376,ad,Andorra,03,Mobiland
      |631,244,ao,Angola,04,MoviCel
      |631,244,ao,Angola,02,Unitel
      |365,1264,ai,Anguilla,840,Cable and Wireless
      |365,1264,ai,Anguilla,010,Digicell / Wireless Vent. Ltd
      |344,1268,ag,Antigua and Barbuda,030,APUA PCS
      |344,1268,ag,Antigua and Barbuda,920,C & W
      |344,1268,ag,Antigua and Barbuda,930,Cing. Wirel./DigiCel
      |722,54,ar,Argentina Republic,310,Claro/ CTI/AMX
      |722,54,ar,Argentina Republic,330,Claro/ CTI/AMX
      |722,54,ar,Argentina Republic,320,Claro/ CTI/AMX
      |722,54,ar,Argentina Republic,010,Compania De Radiocomunicaciones Moviles SA
      |722,54,ar,Argentina Republic,070,Movistar/Telefonica
      |722,54,ar,Argentina Republic,020,Nextel
      |722,54,ar,Argentina Republic,341,Telecom Personal S.A.
      |283,374,am,Armenia,01,ArmenTel/Beeline
      |283,374,am,Armenia,04,Karabakh Telecom
      |283,374,am,Armenia,10,Orange
      |283,374,am,Armenia,05,Vivacell
      |363,297,aw,Aruba,20,Digicel
      |363,297,aw,Aruba,01,Setar GSM
      |505,61,au,Australia,14,AAPT Ltd.
      |505,61,au,Australia,24,Advanced Comm Tech Pty.
      |505,61,au,Australia,09,Airnet Commercial Australia Ltd..
      |505,61,au,Australia,04,Department of Defense
      |505,61,au,Australia,26,Dialogue Communications Pty Ltd
      |505,61,au,Australia,12,H3G Ltd.
      |505,61,au,Australia,06,H3G Ltd.
      |505,61,au,Australia,88,Localstar Holding Pty. Ltd
      |505,61,au,Australia,19,Lycamobile Pty Ltd
      |505,61,au,Australia,08,Railcorp/Vodafone
      |505,61,au,Australia,99,Railcorp/Vodafone
      |505,61,au,Australia,13,Railcorp/Vodafone
      |505,61,au,Australia,90,Singtel Optus
      |505,61,au,Australia,02,Singtel Optus
      |505,61,au,Australia,01,Telstra Corp. Ltd.
      |505,61,au,Australia,11,Telstra Corp. Ltd.
      |505,61,au,Australia,71,Telstra Corp. Ltd.
      |505,61,au,Australia,72,Telstra Corp. Ltd.
      |505,61,au,Australia,05,The Ozitel Network Pty.
      |505,61,au,Australia,16,Victorian Rail Track Corp. (VicTrack)
      |505,61,au,Australia,07,Vodafone
      |505,61,au,Australia,03,Vodafone
      |232,43,at,Austria,02,A1 MobilKom
      |232,43,at,Austria,11,A1 MobilKom
      |232,43,at,Austria,09,A1 MobilKom
      |232,43,at,Austria,01,A1 MobilKom
      |232,43,at,Austria,15,T-Mobile/Telering
      |232,43,at,Austria,00,Fix Line
      |232,43,at,Austria,10,H3G
      |232,43,at,Austria,14,H3G
      |232,43,at,Austria,12,Orange/One Connect
      |232,43,at,Austria,06,Orange/One Connect
      |232,43,at,Austria,05,Orange/One Connect
      |232,43,at,Austria,04,T-Mobile/Telering
      |232,43,at,Austria,03,T-Mobile/Telering
      |232,43,at,Austria,07,T-Mobile/Telering
      |232,43,at,Austria,08,Telefonica
      |400,994,az,Azerbaijan,01,Azercell Telekom B.M.
      |400,994,az,Azerbaijan,04,Azerfon.
      |400,994,az,Azerbaijan,03,Caspian American Telecommunications LLC (CATEL)
      |400,994,az,Azerbaijan,02,J.V. Bakcell GSM 2000
      |364,1242,bs,Bahamas,390,Bahamas Telco. Comp.
      |364,1242,bs,Bahamas,39,Bahamas Telco. Comp.
      |426,973,bh,Bahrain,01,Batelco
      |426,973,bh,Bahrain,02,MTC Vodafone
      |426,973,bh,Bahrain,04,VIVA
      |470,880,bd,Bangladesh,02,Robi/Aktel
      |470,880,bd,Bangladesh,05,Citycell
      |470,880,bd,Bangladesh,06,Citycell
      |470,880,bd,Bangladesh,01,GrameenPhone
      |470,880,bd,Bangladesh,03,Orascom
      |470,880,bd,Bangladesh,04,TeleTalk
      |470,880,bd,Bangladesh,07,Airtel/Warid
      |342,1246,bb,Barbados,600,C & W BET Ltd.
      |342,1246,bb,Barbados,810,Cingular Wireless
      |342,1246,bb,Barbados,750,Digicel
      |342,1246,bb,Barbados,050,Digicel
      |342,1246,bb,Barbados,820,Sunbeach
      |257,375,by,Belarus,03,BelCel JV
      |257,375,by,Belarus,04,BeST
      |257,375,by,Belarus,01,Mobile Digital Communications
      |257,375,by,Belarus,02,MTS
      |206,32,be,Belgium,20,Base/KPN
      |206,32,be,Belgium,01,Belgacom/Proximus
      |206,32,be,Belgium,10,Mobistar/Orange
      |206,32,be,Belgium,02,SNCT/NMBS
      |206,32,be,Belgium,05,Telenet BidCo NV
      |702,501,bz,Belize,67,DigiCell
      |702,501,bz,Belize,68,International Telco (INTELCO)
      |616,229,bj,Benin,04,Bell Benin/BBCOM
      |616,229,bj,Benin,02,Etisalat/MOOV
      |616,229,bj,Benin,05,GloMobile
      |616,229,bj,Benin,01,Libercom
      |616,229,bj,Benin,03,MTN/Spacetel
      |350,1441,bm,Bermuda,000,Bermuda Digital Communications Ltd (BDC)
      |350,1441,bm,Bermuda,99,CellOne Ltd
      |350,1441,bm,Bermuda,10,DigiCel / Cingular
      |350,1441,bm,Bermuda,02,M3 Wireless Ltd
      |350,1441,bm,Bermuda,01,Telecommunications (Bermuda & West Indies) Ltd (Digicel Bermuda)
      |402,975,bt,Bhutan,11,B-Mobile
      |402,975,bt,Bhutan,17,Bhutan Telecom Ltd (BTL)
      |402,975,bt,Bhutan,77,TashiCell
      |736,591,bo,Bolivia,02,Entel Pcs
      |736,591,bo,Bolivia,01,Nuevatel
      |736,591,bo,Bolivia,03,TELECEL BOLIVIA
      |362,599,bq,Bonaire Sint Eustatius and Saba,91,United Telecommunications Services NV (UTS)
      |218,387,ba,Bosnia & Herzegov.,90,BH Mobile
      |218,387,ba,Bosnia & Herzegov.,03,Eronet Mobile
      |218,387,ba,Bosnia & Herzegov.,05,M-Tel
      |652,267,bw,Botswana,04,beMOBILE
      |652,267,bw,Botswana,01,Mascom Wireless (Pty) Ltd.
      |652,267,bw,Botswana,02,Orange
      |724,55,br,Brazil,12,Claro/Albra/America Movil
      |724,55,br,Brazil,38,Claro/Albra/America Movil
      |724,55,br,Brazil,05,Claro/Albra/America Movil
      |724,55,br,Brazil,01,Vivo S.A./Telemig
      |724,55,br,Brazil,33,CTBC Celular SA (CTBC)
      |724,55,br,Brazil,32,CTBC Celular SA (CTBC)
      |724,55,br,Brazil,34,CTBC Celular SA (CTBC)
      |724,55,br,Brazil,08,TIM
      |724,55,br,Brazil,00,Nextel (Telet)
      |724,55,br,Brazil,39,Nextel (Telet)
      |724,55,br,Brazil,30,Oi (TNL PCS / Oi)
      |724,55,br,Brazil,31,Oi (TNL PCS / Oi)
      |724,55,br,Brazil,24,Amazonia Celular S/A
      |724,55,br,Brazil,16,Brazil Telcom
      |724,55,br,Brazil,15,Sercontel Cel
      |724,55,br,Brazil,07,CTBC/Triangulo
      |724,55,br,Brazil,19,Vivo S.A./Telemig
      |724,55,br,Brazil,03,TIM
      |724,55,br,Brazil,02,TIM
      |724,55,br,Brazil,04,TIM
      |724,55,br,Brazil,37,Unicel do Brasil Telecomunicacoes Ltda
      |724,55,br,Brazil,06,Vivo S.A./Telemig
      |724,55,br,Brazil,23,Vivo S.A./Telemig
      |724,55,br,Brazil,11,Vivo S.A./Telemig
      |724,55,br,Brazil,10,Vivo S.A./Telemig
      |348,284,vg,British Virgin Islands,570,Caribbean Cellular
      |348,284,vg,British Virgin Islands,770,Digicel
      |348,284,vg,British Virgin Islands,170,LIME
      |528,673,bn,Brunei Darussalam,02,b-mobile
      |528,673,bn,Brunei Darussalam,11,Datastream (DTSCom)
      |528,673,bn,Brunei Darussalam,01,Telekom Brunei Bhd (TelBru)
      |284,359,bg,Bulgaria,06,BTC Mobile EOOD (vivatel)
      |284,359,bg,Bulgaria,03,BTC Mobile EOOD (vivatel)
      |284,359,bg,Bulgaria,05,Cosmo Mobile EAD/Globul
      |284,359,bg,Bulgaria,01,MobilTel AD
      |613,226,bf,Burkina Faso,03,TeleCel
      |613,226,bf,Burkina Faso,01,TeleMob-OnaTel
      |613,226,bf,Burkina Faso,02,AirTel/ZAIN/CelTel
      |414,95,mm,Burma/Myanmar,01,Myanmar Post & Teleco.
      |642,257,bi,Burundi,02,Africel / Safaris
      |642,257,bi,Burundi,08,HiTs Telecom
      |642,257,bi,Burundi,03,Onatel / Telecel
      |642,257,bi,Burundi,07,Smart Mobile / LACELL
      |642,257,bi,Burundi,01,Spacetel / Econet
      |642,257,bi,Burundi,82,U-COM
      |456,855,kh,Cambodia,04,Cambodia Advance Communications Co. Ltd (CADCOMMS)
      |456,855,kh,Cambodia,02,Hello/Malaysia Telcom
      |456,855,kh,Cambodia,08,Metfone
      |456,855,kh,Cambodia,18,MFone/Camshin
      |456,855,kh,Cambodia,01,Mobitel/Cam GSM
      |456,855,kh,Cambodia,03,QB/Cambodia Adv. Comms.
      |456,855,kh,Cambodia,05,Smart Mobile
      |456,855,kh,Cambodia,06,Smart Mobile
      |456,855,kh,Cambodia,09,Sotelco Ltd (Beeline Cambodia)
      |624,237,cm,Cameroon,01,MTN
      |624,237,cm,Cameroon,02,Orange
      |302,1,ca,Canada,652,BC Tel Mobility
      |302,1,ca,Canada,630,Bell Aliant
      |302,1,ca,Canada,651,Bell Mobility
      |302,1,ca,Canada,610,Bell Mobility
      |302,1,ca,Canada,670,CityWest Mobility
      |302,1,ca,Canada,360,Clearnet
      |302,1,ca,Canada,361,Clearnet
      |302,1,ca,Canada,380,DMTS Mobility
      |302,1,ca,Canada,710,Globalstar Canada
      |302,1,ca,Canada,640,Latitude Wireless
      |302,1,ca,Canada,370,FIDO (Rogers AT&T/ Microcell)
      |302,1,ca,Canada,320,mobilicity
      |302,1,ca,Canada,702,MT&T Mobility
      |302,1,ca,Canada,655,MTS Mobility
      |302,1,ca,Canada,660,MTS Mobility
      |302,1,ca,Canada,701,NB Tel Mobility
      |302,1,ca,Canada,703,New Tel Mobility
      |302,1,ca,Canada,760,Public Mobile
      |302,1,ca,Canada,657,Quebectel Mobility
      |302,1,ca,Canada,720,Rogers AT&T Wireless
      |302,1,ca,Canada,654,Sask Tel Mobility
      |302,1,ca,Canada,680,Sask Tel Mobility
      |302,1,ca,Canada,656,Tbay Mobility
      |302,1,ca,Canada,220,Telus Mobility
      |302,1,ca,Canada,653,Telus Mobility
      |302,1,ca,Canada,500,Videotron
      |302,1,ca,Canada,490,WIND
      |625,238,cv,Cape Verde,01,CV Movel
      |625,238,cv,Cape Verde,02,T+ Telecom
      |346,1345,ky,Cayman Islands,050,Digicel Cayman Ltd
      |346,1345,ky,Cayman Islands,006,Digicel Ltd.
      |346,1345,ky,Cayman Islands,140,LIME / Cable & Wirel.
      |623,236,cf,Central African Rep.,01,Centrafr. Telecom+
      |623,236,cf,Central African Rep.,04,Nationlink
      |623,236,cf,Central African Rep.,03,Orange/Celca
      |623,236,cf,Central African Rep.,02,Telecel Centraf.
      |622,235,td,Chad,04,Salam/Sotel
      |622,235,td,Chad,02,Tchad Mobile
      |622,235,td,Chad,03,Tigo/Milicom/Tchad Mobile
      |622,235,td,Chad,01,Zain/Airtel/Celtel
      |730,56,cl,Chile,06,Blue Two Chile SA
      |730,56,cl,Chile,11,Celupago SA
      |730,56,cl,Chile,15,Cibeles Telecom SA
      |730,56,cl,Chile,03,Claro
      |730,56,cl,Chile,10,Entel PCS
      |730,56,cl,Chile,01,Entel Telefonia Mov
      |730,56,cl,Chile,14,Netline Telefonica Movil Ltda
      |730,56,cl,Chile,09,Nextel SA
      |730,56,cl,Chile,05,Nextel SA
      |730,56,cl,Chile,04,Nextel SA
      |730,56,cl,Chile,02,TELEFONICA
      |730,56,cl,Chile,07,TELEFONICA
      |730,56,cl,Chile,12,Telestar Movil SA
      |730,56,cl,Chile,00,TESAM SA
      |730,56,cl,Chile,13,Tribe Mobile SPA
      |730,56,cl,Chile,08,VTR Banda Ancha SA
      |460,86,cn,China,00,China Mobile GSM
      |460,86,cn,China,07,China Mobile GSM
      |460,86,cn,China,02,China Mobile GSM
      |460,86,cn,China,04,China Space Mobile Satellite Telecommunications Co. Ltd (China Spacecom)
      |460,86,cn,China,05,China Telecom
      |460,86,cn,China,03,China Telecom
      |460,86,cn,China,06,China Unicom
      |460,86,cn,China,01,China Unicom
      |732,57,co,Colombia,130,Avantel SAS
      |732,57,co,Colombia,102,Movistar
      |732,57,co,Colombia,103,TIGO/Colombia Movil
      |732,57,co,Colombia,001,TIGO/Colombia Movil
      |732,57,co,Colombia,101,Comcel S.A. Occel S.A./Celcaribe
      |732,57,co,Colombia,002,Edatel S.A.
      |732,57,co,Colombia,123,Movistar
      |732,57,co,Colombia,111,TIGO/Colombia Movil
      |732,57,co,Colombia,142,UNE EPM Telecomunicaciones SA ESP
      |732,57,co,Colombia,020,UNE EPM Telecomunicaciones SA ESP
      |732,57,co,Colombia,154,Virgin Mobile Colombia SAS
      |654,269,km,Comoros,01,HURI - SNPT
      |630,243,cd,Congo Dem. Rep.,86,Orange RDC sarl
      |630,243,cd,Congo Dem. Rep.,05,SuperCell
      |630,243,cd,Congo Dem. Rep.,89,TIGO/Oasis
      |630,243,cd,Congo Dem. Rep.,01,Vodacom
      |630,243,cd,Congo Dem. Rep.,88,Yozma Timeturns sprl (YTT)
      |630,243,cd,Congo Dem. Rep.,02,ZAIN CelTel
      |629,242,cg,Congo Republic,01,Airtel Congo SA
      |629,242,cg,Congo Republic,02,Zain/Celtel
      |629,242,cg,Congo Republic,10,MTN/Libertis
      |629,242,cg,Congo Republic,07,Warid
      |548,682,ck,Cook Islands,01,Telecom Cook Islands
      |712,506,cr,Costa Rica,03,Claro
      |712,506,cr,Costa Rica,02,ICE
      |712,506,cr,Costa Rica,01,ICE
      |712,506,cr,Costa Rica,04,Movistar
      |219,385,hr,Croatia,01,T-Mobile/Cronet
      |219,385,hr,Croatia,02,Tele2
      |219,385,hr,Croatia,10,VIPnet d.o.o.
      |368,53,cu,Cuba,01,C-COM
      |362,599,cw,Curacao,95,EOCG Wireless NV
      |362,599,cw,Curacao,69,Polycom N.V./ Curacao Telecom d.b.a. Digicel
      |280,357,cy,Cyprus,10,MTN/Areeba
      |280,357,cy,Cyprus,20,PrimeTel PLC
      |280,357,cy,Cyprus,01,Vodafone/CyTa
      |230,420,cz,Czech Rep.,08,Compatel s.r.o.
      |230,420,cz,Czech Rep.,02,O2
      |230,420,cz,Czech Rep.,01,T-Mobile / RadioMobil
      |230,420,cz,Czech Rep.,05,Travel Telekommunikation s.r.o.
      |230,420,cz,Czech Rep.,04,Ufone
      |230,420,cz,Czech Rep.,99,Vodafone
      |230,420,cz,Czech Rep.,03,Vodafone
      |238,45,dk,Denmark,05,ApS KBUS
      |238,45,dk,Denmark,23,Banedanmark
      |238,45,dk,Denmark,28,CoolTEL ApS
      |238,45,dk,Denmark,06,Hi3G
      |238,45,dk,Denmark,12,Lycamobile Ltd
      |238,45,dk,Denmark,03,Mach Connectivity ApS
      |238,45,dk,Denmark,07,
      |238,45,dk,Denmark,04,NextGen Mobile Ltd (CardBoardFish)
      |238,45,dk,Denmark,10,TDC Denmark
      |238,45,dk,Denmark,01,TDC Denmark
      |238,45,dk,Denmark,02,Telenor/Sonofon
      |238,45,dk,Denmark,77,Telenor/Sonofon
      |238,45,dk,Denmark,20,Telia
      |238,45,dk,Denmark,30,Telia
      |638,253,dj,Djibouti,01,Djibouti Telecom SA (Evatis)
      |366,1767,dm,Dominica,110,C & W
      |366,1767,dm,Dominica,020,Cingular Wireless/Digicel
      |366,1767,dm,Dominica,050,Wireless Ventures (Dominica) Ltd (Digicel Dominica)
      |370,1809,do,Dominican Republic,02,Claro
      |370,1809,do,Dominican Republic,01,Orange
      |370,1809,do,Dominican Republic,03,TRIcom
      |370,1809,do,Dominican Republic,04,Trilogy Dominicana S. A.
      |740,593,ec,Ecuador,02,Alegro/Telcsa
      |740,593,ec,Ecuador,00,MOVISTAR/OteCel
      |740,593,ec,Ecuador,01,Porta/Conecel
      |602,20,eg,Egypt,01,EMS - Mobinil
      |602,20,eg,Egypt,03,ETISALAT
      |602,20,eg,Egypt,02,Vodafone (Misrfone Telecom)
      |706,503,sv,El Salvador,01,CLARO/CTE
      |706,503,sv,El Salvador,02,Digicel
      |706,503,sv,El Salvador,05,INTELFON SA de CV
      |706,503,sv,El Salvador,04,Telefonica
      |706,503,sv,El Salvador,03,Telemovil
      |627,240,gq,Equatorial Guinea,03,HiTs-GE
      |627,240,gq,Equatorial Guinea,01,ORANGE/GETESA
      |657,291,er,Eritrea,01,Eritel
      |248,372,ee,Estonia,01,EMT GSM
      |248,372,ee,Estonia,02,Radiolinja Eesti
      |248,372,ee,Estonia,03,Tele2 Eesti AS
      |248,372,ee,Estonia,04,Top Connect OU
      |636,251,et,Ethiopia,01,ETH/MTN
      |750,500,fk,Falkland Islands (Malvinas),001,Cable and Wireless South Atlantic Ltd (Falkland Islands
      |288,298,fo,Faroe Islands,03,Edge Mobile Sp/F
      |288,298,fo,Faroe Islands,01,Faroese Telecom
      |288,298,fo,Faroe Islands,02,Kall GSM
      |542,679,fj,Fiji,02,DigiCell
      |542,679,fj,Fiji,01,Vodafone
      |244,358,fi,Finland,14,Alands
      |244,358,fi,Finland,26,Compatel Ltd
      |244,358,fi,Finland,13,DNA/Finnet
      |244,358,fi,Finland,12,DNA/Finnet
      |244,358,fi,Finland,04,DNA/Finnet
      |244,358,fi,Finland,03,DNA/Finnet
      |244,358,fi,Finland,21,Elisa/Saunalahti
      |244,358,fi,Finland,05,Elisa/Saunalahti
      |244,358,fi,Finland,82,ID-Mobile
      |244,358,fi,Finland,11,Mundio Mobile (Finland) Ltd
      |244,358,fi,Finland,09,Nokia Oyj
      |244,358,fi,Finland,10,TDC Oy Finland
      |244,358,fi,Finland,91,TeliaSonera
      |208,33,fr,France,27,AFONE SA
      |208,33,fr,France,92,Association Plate-forme Telecom
      |208,33,fr,France,28,Astrium
      |208,33,fr,France,88,Bouygues Telecom
      |208,33,fr,France,21,Bouygues Telecom
      |208,33,fr,France,20,Bouygues Telecom
      |208,33,fr,France,14,Lliad/FREE Mobile
      |208,33,fr,France,05,GlobalStar
      |208,33,fr,France,07,GlobalStar
      |208,33,fr,France,06,GlobalStar
      |208,33,fr,France,29,Orange
      |208,33,fr,France,16,Lliad/FREE Mobile
      |208,33,fr,France,15,Lliad/FREE Mobile
      |208,33,fr,France,25,Lycamobile SARL
      |208,33,fr,France,24,MobiquiThings
      |208,33,fr,France,03,MobiquiThings
      |208,33,fr,France,31,Mundio Mobile (France) Ltd
      |208,33,fr,France,26,NRJ
      |208,33,fr,France,89,Omer/Virgin Mobile
      |208,33,fr,France,23,Omer/Virgin Mobile
      |208,33,fr,France,02,Orange
      |208,33,fr,France,01,Orange
      |208,33,fr,France,91,Orange
      |208,33,fr,France,13,S.F.R.
      |208,33,fr,France,11,S.F.R.
      |208,33,fr,France,10,S.F.R.
      |208,33,fr,France,09,S.F.R.
      |208,33,fr,France,04,SISTEER
      |208,33,fr,France,00,Tel/Tel
      |208,33,fr,France,22,Transatel SA
      |340,594,gf,French Guiana,20,Bouygues/DigiCel
      |340,594,gf,French Guiana,08,AMIGO/Dauphin
      |340,594,gf,French Guiana,01,Orange Caribe
      |340,594,gf,French Guiana,02,Outremer Telecom
      |340,594,gf,French Guiana,11,TelCell GSM
      |340,594,gf,French Guiana,03,TelCell GSM
      |547,689,pf,French Polynesia,15,Pacific Mobile Telecom (PMT)
      |547,689,pf,French Polynesia,20,Tikiphone
      |628,241,ga,Gabon,04,Azur/Usan S.A.
      |628,241,ga,Gabon,01,Libertis S.A.
      |628,241,ga,Gabon,02,MOOV/Telecel
      |628,241,ga,Gabon,03,ZAIN/Celtel Gabon S.A.
      |607,220,gm,Gambia,02,Africel
      |607,220,gm,Gambia,03,Comium
      |607,220,gm,Gambia,01,Gamcel
      |607,220,gm,Gambia,04,Q-Cell
      |282,995,ge,Georgia,01,Geocell Ltd.
      |282,995,ge,Georgia,03,Iberiatel Ltd.
      |282,995,ge,Georgia,02,Magti GSM Ltd.
      |282,995,ge,Georgia,04,MobiTel/Beeline
      |282,995,ge,Georgia,05,Silknet
      |262,49,de,Germany,17,E-Plus
      |262,49,de,Germany,901,Debitel
      |262,49,de,Germany,03,E-Plus
      |262,49,de,Germany,05,E-Plus
      |262,49,de,Germany,77,E-Plus
      |262,49,de,Germany,14,Group 3G UMTS
      |262,49,de,Germany,43,Lycamobile
      |262,49,de,Germany,13,Mobilcom
      |262,49,de,Germany,07,O2
      |262,49,de,Germany,11,O2
      |262,49,de,Germany,08,O2
      |262,49,de,Germany,10,O2
      |262,49,de,Germany,12,O2
      |262,49,de,Germany,000,Talkline
      |262,49,de,Germany,06,Telekom/T-mobile
      |262,49,de,Germany,01,Telekom/T-mobile
      |262,49,de,Germany,16,Telogic/ViStream
      |262,49,de,Germany,04,Vodafone D2
      |262,49,de,Germany,02,Vodafone D2
      |262,49,de,Germany,09,Vodafone D2
      |620,233,gh,Ghana,04,Expresso Ghana Ltd
      |620,233,gh,Ghana,07,GloMobile
      |620,233,gh,Ghana,03,Milicom/Tigo
      |620,233,gh,Ghana,01,MTN
      |620,233,gh,Ghana,02,Vodafone
      |620,233,gh,Ghana,06,ZAIN
      |266,350,gi,Gibraltar,06,CTS Mobile
      |266,350,gi,Gibraltar,09,eazi telecom
      |266,350,gi,Gibraltar,01,Gibtel GSM
      |202,30,gr,Greece,07,AMD Telecom SA
      |202,30,gr,Greece,02,Cosmote
      |202,30,gr,Greece,01,Cosmote
      |202,30,gr,Greece,04,Organismos Sidirodromon Ellados (OSE)
      |202,30,gr,Greece,03,OTE Hellenic Telecommunications Organization SA
      |202,30,gr,Greece,10,Tim/Wind
      |202,30,gr,Greece,09,Tim/Wind
      |202,30,gr,Greece,05,Vodafone
      |290,299,gl,Greenland,01,Tele Greenland
      |352,1473,gd,Grenada,110,Cable & Wireless
      |352,1473,gd,Grenada,030,Digicel
      |352,1473,gd,Grenada,050,Digicel
      |340,590,gp,Guadeloupe,08,Dauphin Telecom SU (Guadeloupe Telecom) (Guadeloupe
      |340,590,gp,Guadeloupe,20,Digicel Antilles Francaises Guyane SA (Guadeloupe
      |340,590,gp,Guadeloupe,01,Orange Caribe
      |340,590,gp,Guadeloupe,02,Outremer Telecom Guadeloupe (only) (Guadeloupe
      |340,590,gp,Guadeloupe,10,United Telecommunications Services Caraibe SARL (UTS Caraibe Guadeloupe """ +
        """Telephone Mobile) (Guadeloupe)
      |340,590,gp,Guadeloupe,03,United Telecommunications Services Caraibe SARL (UTS Caraibe Guadeloupe """ +
        """Telephone Mobile) (Guadeloupe)
      |310,1671,gu,Guam,480,Choice Phone LLC
      |310,1671,gu,Guam,370,Docomo
      |310,1671,gu,Guam,470,Docomo
      |310,1671,gu,Guam,140,GTA Wireless
      |310,1671,gu,Guam,033,Guam Teleph. Auth.
      |310,1671,gu,Guam,032,IT&E OverSeas
      |311,1671,gu,Guam,250,Wave Runner LLC
      |704,502,gt,Guatemala,01,SERCOM
      |704,502,gt,Guatemala,03,Telefonica
      |704,502,gt,Guatemala,02,TIGO/COMCEL
      |611,224,gn,Guinea,04,Areeba - MTN
      |611,224,gn,Guinea,05,Celcom
      |611,224,gn,Guinea,03,Intercel
      |611,224,gn,Guinea,01,Orange/Spacetel
      |611,224,gn,Guinea,02,SotelGui
      |632,245,gw,Guinea-Bissau,07,GuineTel
      |632,245,gw,Guinea-Bissau,01,GuineTel
      |632,245,gw,Guinea-Bissau,03,Orange
      |632,245,gw,Guinea-Bissau,02,SpaceTel
      |738,592,gy,Guyana,02,Cellink Plus
      |738,592,gy,Guyana,01,DigiCel
      |372,509,ht,Haiti,01,Comcel
      |372,509,ht,Haiti,02,Digicel
      |372,509,ht,Haiti,03,National Telecom SA (NatCom)
      |708,504,hn,Honduras,040,Digicel
      |708,504,hn,Honduras,030,HonduTel
      |708,504,hn,Honduras,001,SERCOM/CLARO
      |708,504,hn,Honduras,002,Telefonica/CELTEL
      |454,852,hk,Hongkong China,13,China Mobile/Peoples
      |454,852,hk,Hongkong China,12,China Mobile/Peoples
      |454,852,hk,Hongkong China,09,China Motion
      |454,852,hk,Hongkong China,07,China Unicom Ltd
      |454,852,hk,Hongkong China,11,China-HongKong Telecom Ltd (CHKTL)
      |454,852,hk,Hongkong China,01,Citic Telecom Ltd.
      |454,852,hk,Hongkong China,18,CSL Ltd.
      |454,852,hk,Hongkong China,02,CSL Ltd.
      |454,852,hk,Hongkong China,00,CSL Ltd.
      |454,852,hk,Hongkong China,10,CSL/New World PCS Ltd.
      |454,852,hk,Hongkong China,14,H3G/Hutchinson
      |454,852,hk,Hongkong China,05,H3G/Hutchinson
      |454,852,hk,Hongkong China,04,H3G/Hutchinson
      |454,852,hk,Hongkong China,03,H3G/Hutchinson
      |454,852,hk,Hongkong China,16,HKT/PCCW
      |454,852,hk,Hongkong China,19,HKT/PCCW
      |454,852,hk,Hongkong China,20,HKT/PCCW
      |454,852,hk,Hongkong China,29,HKT/PCCW
      |454,852,hk,Hongkong China,47,shared by private TETRA systems
      |454,852,hk,Hongkong China,40,shared by private TETRA systems
      |454,852,hk,Hongkong China,08,Trident Telecom Ventures Ltd.
      |454,852,hk,Hongkong China,17,Vodafone/SmarTone
      |454,852,hk,Hongkong China,15,Vodafone/SmarTone
      |454,852,hk,Hongkong China,06,Vodafone/SmarTone
      |216,36,hu,Hungary,01,Pannon/Telenor
      |216,36,hu,Hungary,30,T-mobile/Magyar
      |216,36,hu,Hungary,71,UPC Magyarorszag Kft.
      |216,36,hu,Hungary,70,Vodafone
      |274,354,is,Iceland,09,Amitelo
      |274,354,is,Iceland,07,IceCell
      |274,354,is,Iceland,08,Landssiminn
      |274,354,is,Iceland,01,Landssiminn
      |274,354,is,Iceland,11,NOVA
      |274,354,is,Iceland,04,VIKING/IMC
      |274,354,is,Iceland,03,Vodafone/Tal hf
      |274,354,is,Iceland,05,Vodafone/Tal hf
      |274,354,is,Iceland,02,Vodafone/Tal hf
      |404,91,in,India,29,Aircel
      |404,91,in,India,28,Aircel
      |404,91,in,India,25,Aircel
      |404,91,in,India,17,Aircel
      |404,91,in,India,42,Aircel
      |404,91,in,India,33,Aircel
      |404,91,in,India,01,Aircel Digilink India
      |404,91,in,India,15,Aircel Digilink India
      |404,91,in,India,60,Aircel Digilink India
      |405,91,in,India,55,AirTel
      |405,91,in,India,53,AirTel
      |405,91,in,India,51,AirTel
      |405,91,in,India,56,Airtel (Bharati Mobile) - Assam
      |404,91,in,India,86,Barakhamba Sales & Serv.
      |404,91,in,India,13,Barakhamba Sales & Serv.
      |404,91,in,India,58,BSNL
      |404,91,in,India,81,BSNL
      |404,91,in,India,74,BSNL
      |404,91,in,India,38,BSNL
      |404,91,in,India,57,BSNL
      |404,91,in,India,80,BSNL
      |404,91,in,India,73,BSNL
      |404,91,in,India,34,BSNL
      |404,91,in,India,66,BSNL
      |404,91,in,India,55,BSNL
      |404,91,in,India,72,BSNL
      |404,91,in,India,77,BSNL
      |404,91,in,India,64,BSNL
      |404,91,in,India,54,BSNL
      |404,91,in,India,71,BSNL
      |404,91,in,India,76,BSNL
      |404,91,in,India,53,BSNL
      |404,91,in,India,62,BSNL
      |404,91,in,India,59,BSNL
      |404,91,in,India,75,BSNL
      |404,91,in,India,51,BSNL
      |405,91,in,India,10,Bharti Airtel Limited (Delhi)
      |404,91,in,India,79,CellOne A&N
      |404,91,in,India,89,Escorts Telecom Ltd.
      |404,91,in,India,88,Escorts Telecom Ltd.
      |404,91,in,India,87,Escorts Telecom Ltd.
      |404,91,in,India,82,Escorts Telecom Ltd.
      |404,91,in,India,12,Escotel Mobile Communications
      |404,91,in,India,19,Escotel Mobile Communications
      |404,91,in,India,56,Escotel Mobile Communications
      |405,91,in,India,05,Fascel Limited
      |404,91,in,India,05,Fascel
      |404,91,in,India,70,Hexacom India
      |404,91,in,India,16,Hexcom India
      |404,91,in,India,04,Idea Cellular Ltd.
      |404,91,in,India,24,Idea Cellular Ltd.
      |404,91,in,India,22,Idea Cellular Ltd.
      |404,91,in,India,78,Idea Cellular Ltd.
      |404,91,in,India,07,Idea Cellular Ltd.
      |404,91,in,India,69,Mahanagar Telephone Nigam
      |404,91,in,India,68,Mahanagar Telephone Nigam
      |404,91,in,India,83,Reliable Internet Services
      |405,91,in,India,09,RELIANCE TELECOM
      |404,91,in,India,36,Reliance Telecom Private
      |404,91,in,India,52,Reliance Telecom Private
      |404,91,in,India,50,Reliance Telecom Private
      |404,91,in,India,67,Reliance Telecom Private
      |404,91,in,India,18,Reliance Telecom Private
      |404,91,in,India,85,Reliance Telecom Private
      |404,91,in,India,09,Reliance Telecom Private
      |404,91,in,India,41,RPG Cellular
      |404,91,in,India,14,Spice
      |404,91,in,India,44,Spice
      |404,91,in,India,11,Sterling Cellular Ltd.
      |404,91,in,India,30,Usha Martin Telecom
      |510,62,id,Indonesia,08,Axis/Natrindo
      |510,62,id,Indonesia,89,H3G CP
      |510,62,id,Indonesia,21,Indosat/Satelindo/M3
      |510,62,id,Indonesia,01,Indosat/Satelindo/M3
      |510,62,id,Indonesia,00,PT Pasifik Satelit Nusantara (PSN)
      |510,62,id,Indonesia,27,PT Sampoerna Telekomunikasi Indonesia (STI)
      |510,62,id,Indonesia,09,PT Smartfren Telecom Tbk
      |510,62,id,Indonesia,28,PT Smartfren Telecom Tbk
      |510,62,id,Indonesia,11,PT. Excelcom
      |510,62,id,Indonesia,10,Telkomsel
      |901,882,n/a,International Networks,13,Antarctica
      |432,98,ir,Iran,19,Mobile Telecommunications Company of Esfahan JV-PJS (MTCE)
      |432,98,ir,Iran,70,MTCE
      |432,98,ir,Iran,35,MTN/IranCell
      |432,98,ir,Iran,32,Taliya
      |432,98,ir,Iran,11,TCI / MCI
      |432,98,ir,Iran,14,TKC/KFZO
      |418,964,iq,Iraq,05,Asia Cell
      |418,964,iq,Iraq,92,Itisaluna and Kalemat
      |418,964,iq,Iraq,82,Korek
      |418,964,iq,Iraq,40,Korek
      |418,964,iq,Iraq,45,Mobitel (Iraq-Kurdistan) and Moutiny
      |418,964,iq,Iraq,30,Orascom Telecom
      |418,964,iq,Iraq,20,ZAIN/Atheer
      |418,964,iq,Iraq,08,Sanatel
      |272,353,ie,Ireland,04,Access Telecom Ltd.
      |272,353,ie,Ireland,09,Clever Communications Ltd
      |272,353,ie,Ireland,07,eircom Ltd
      |272,353,ie,Ireland,05,H3G
      |272,353,ie,Ireland,11,Liffey Telecom
      |272,353,ie,Ireland,13,Lycamobile
      |272,353,ie,Ireland,03,Meteor Mobile Ltd.
      |272,353,ie,Ireland,02,O2/Digifone
      |272,353,ie,Ireland,01,Vodafone Eircell
      |425,972,il,Israel,14,Alon Cellular Ltd
      |425,972,il,Israel,02,Cellcom ltd.
      |425,972,il,Israel,08,Golan Telekom
      |425,972,il,Israel,15,Home Cellular Ltd
      |425,972,il,Israel,77,Hot Mobile/Mirs
      |425,972,il,Israel,07,Hot Mobile/Mirs
      |425,972,il,Israel,01,Orange/Partner Co. Ltd.
      |425,972,il,Israel,03,Pelephone
      |425,972,il,Israel,16,Rami Levy Hashikma Marketing Communications Ltd
      |222,39,it,Italy,34,BT Italia SpA
      |222,39,it,Italy,02,Elsacom
      |222,39,it,Italy,99,Hi3G
      |222,39,it,Italy,33,Hi3G
      |222,39,it,Italy,77,IPSE 2000
      |222,39,it,Italy,35,Lycamobile Srl
      |222,39,it,Italy,07,Noverca Italia Srl
      |222,39,it,Italy,30,RFI Rete Ferroviaria Italiana SpA
      |222,39,it,Italy,48,Telecom Italia Mobile SpA
      |222,39,it,Italy,43,Telecom Italia Mobile SpA
      |222,39,it,Italy,01,TIM
      |222,39,it,Italy,10,Vodafone
      |222,39,it,Italy,06,Vodafone
      |222,39,it,Italy,44,WIND (Blu) -
      |222,39,it,Italy,88,WIND (Blu) -
      |612,225,ci,Ivory Coast,07,Aircomm SA
      |612,225,ci,Ivory Coast,02,Atlantik Tel./Moov
      |612,225,ci,Ivory Coast,04,Comium
      |612,225,ci,Ivory Coast,01,Comstar
      |612,225,ci,Ivory Coast,05,MTN
      |612,225,ci,Ivory Coast,03,Orange
      |612,225,ci,Ivory Coast,06,OriCell
      |338,1876,jm,Jamaica,110,Cable & Wireless
      |338,1876,jm,Jamaica,020,Cable & Wireless
      |338,1876,jm,Jamaica,180,Cable & Wireless
      |338,1876,jm,Jamaica,050,DIGICEL/Mossel
      |440,81,jp,Japan,00,eMobile
      |440,81,jp,Japan,74,KDDI Corporation
      |440,81,jp,Japan,70,KDDI Corporation
      |440,81,jp,Japan,89,KDDI Corporation
      |440,81,jp,Japan,51,KDDI Corporation
      |440,81,jp,Japan,75,KDDI Corporation
      |440,81,jp,Japan,56,KDDI Corporation
      |441,81,jp,Japan,70,KDDI Corporation
      |440,81,jp,Japan,52,KDDI Corporation
      |440,81,jp,Japan,76,KDDI Corporation
      |440,81,jp,Japan,71,KDDI Corporation
      |440,81,jp,Japan,53,KDDI Corporation
      |440,81,jp,Japan,77,KDDI Corporation
      |440,81,jp,Japan,08,KDDI Corporation
      |440,81,jp,Japan,72,KDDI Corporation
      |440,81,jp,Japan,54,KDDI Corporation
      |440,81,jp,Japan,79,KDDI Corporation
      |440,81,jp,Japan,07,KDDI Corporation
      |440,81,jp,Japan,73,KDDI Corporation
      |440,81,jp,Japan,55,KDDI Corporation
      |440,81,jp,Japan,88,KDDI Corporation
      |440,81,jp,Japan,50,KDDI Corporation
      |440,81,jp,Japan,21,NTT Docomo
      |441,81,jp,Japan,44,NTT Docomo
      |440,81,jp,Japan,13,NTT Docomo
      |440,81,jp,Japan,23,NTT Docomo
      |440,81,jp,Japan,16,NTT Docomo
      |441,81,jp,Japan,99,NTT Docomo
      |440,81,jp,Japan,34,NTT Docomo
      |440,81,jp,Japan,69,NTT Docomo
      |440,81,jp,Japan,64,NTT Docomo
      |440,81,jp,Japan,37,NTT Docomo
      |440,81,jp,Japan,25,NTT Docomo
      |440,81,jp,Japan,22,NTT Docomo
      |441,81,jp,Japan,43,NTT Docomo
      |440,81,jp,Japan,27,NTT Docomo
      |440,81,jp,Japan,02,NTT Docomo
      |440,81,jp,Japan,17,NTT Docomo
      |440,81,jp,Japan,31,NTT Docomo
      |440,81,jp,Japan,87,NTT Docomo
      |440,81,jp,Japan,65,NTT Docomo
      |440,81,jp,Japan,36,NTT Docomo
      |441,81,jp,Japan,92,NTT Docomo
      |440,81,jp,Japan,12,NTT Docomo
      |440,81,jp,Japan,58,NTT Docomo
      |440,81,jp,Japan,28,NTT Docomo
      |440,81,jp,Japan,03,NTT Docomo
      |440,81,jp,Japan,18,NTT Docomo
      |441,81,jp,Japan,91,NTT Docomo
      |440,81,jp,Japan,32,NTT Docomo
      |440,81,jp,Japan,61,NTT Docomo
      |440,81,jp,Japan,66,NTT Docomo
      |440,81,jp,Japan,35,NTT Docomo
      |441,81,jp,Japan,93,NTT Docomo
      |441,81,jp,Japan,40,NTT Docomo
      |440,81,jp,Japan,49,NTT Docomo
      |440,81,jp,Japan,29,NTT Docomo
      |440,81,jp,Japan,09,NTT Docomo
      |440,81,jp,Japan,19,NTT Docomo
      |441,81,jp,Japan,90,NTT Docomo
      |440,81,jp,Japan,33,NTT Docomo
      |440,81,jp,Japan,60,NTT Docomo
      |440,81,jp,Japan,14,NTT Docomo
      |441,81,jp,Japan,94,NTT Docomo
      |441,81,jp,Japan,41,NTT Docomo
      |440,81,jp,Japan,67,NTT Docomo
      |440,81,jp,Japan,62,NTT Docomo
      |440,81,jp,Japan,01,NTT Docomo
      |440,81,jp,Japan,39,NTT Docomo
      |440,81,jp,Japan,30,NTT Docomo
      |440,81,jp,Japan,10,NTT Docomo
      |440,81,jp,Japan,20,NTT Docomo
      |441,81,jp,Japan,45,NTT Docomo
      |440,81,jp,Japan,24,NTT Docomo
      |440,81,jp,Japan,15,NTT Docomo
      |441,81,jp,Japan,98,NTT Docomo
      |441,81,jp,Japan,42,NTT Docomo
      |440,81,jp,Japan,68,NTT Docomo
      |440,81,jp,Japan,63,NTT Docomo
      |440,81,jp,Japan,38,NTT Docomo
      |440,81,jp,Japan,26,NTT Docomo
      |440,81,jp,Japan,11,NTT Docomo
      |440,81,jp,Japan,99,NTT Docomo
      |440,81,jp,Japan,78,Okinawa Cellular Telephone
      |440,81,jp,Japan,47,SoftBank Mobile Corp
      |440,81,jp,Japan,95,SoftBank Mobile Corp
      |440,81,jp,Japan,41,SoftBank Mobile Corp
      |441,81,jp,Japan,64,SoftBank Mobile Corp
      |440,81,jp,Japan,46,SoftBank Mobile Corp
      |440,81,jp,Japan,97,SoftBank Mobile Corp
      |440,81,jp,Japan,42,SoftBank Mobile Corp
      |441,81,jp,Japan,65,SoftBank Mobile Corp
      |440,81,jp,Japan,90,SoftBank Mobile Corp
      |440,81,jp,Japan,92,SoftBank Mobile Corp
      |440,81,jp,Japan,98,SoftBank Mobile Corp
      |440,81,jp,Japan,43,SoftBank Mobile Corp
      |440,81,jp,Japan,48,SoftBank Mobile Corp
      |440,81,jp,Japan,93,SoftBank Mobile Corp
      |440,81,jp,Japan,06,SoftBank Mobile Corp
      |441,81,jp,Japan,61,SoftBank Mobile Corp
      |440,81,jp,Japan,44,SoftBank Mobile Corp
      |440,81,jp,Japan,04,SoftBank Mobile Corp
      |440,81,jp,Japan,94,SoftBank Mobile Corp
      |441,81,jp,Japan,62,SoftBank Mobile Corp
      |440,81,jp,Japan,45,SoftBank Mobile Corp
      |440,81,jp,Japan,40,SoftBank Mobile Corp
      |440,81,jp,Japan,96,SoftBank Mobile Corp
      |441,81,jp,Japan,63,SoftBank Mobile Corp
      |440,81,jp,Japan,85,KDDI Corporation
      |440,81,jp,Japan,83,KDDI Corporation
      |440,81,jp,Japan,81,KDDI Corporation
      |440,81,jp,Japan,80,KDDI Corporation
      |440,81,jp,Japan,86,KDDI Corporation
      |440,81,jp,Japan,84,KDDI Corporation
      |440,81,jp,Japan,82,KDDI Corporation
      |416,962,jo,Jordan,77,Orange/Petra
      |416,962,jo,Jordan,03,Umniah Mobile Co.
      |416,962,jo,Jordan,02,Xpress
      |416,962,jo,Jordan,01,ZAIN /J.M.T.S
      |401,7,kz,Kazakhstan,01,Beeline/KaR-Tel LLP
      |401,7,kz,Kazakhstan,07,Dalacom/Altel
      |401,7,kz,Kazakhstan,02,K-Cell
      |401,7,kz,Kazakhstan,77,NEO/MTS
      |639,254,ke,Kenya,05,Econet Wireless
      |639,254,ke,Kenya,07,Orange
      |639,254,ke,Kenya,02,Safaricom Ltd.
      |639,254,ke,Kenya,03,Zain/Celtel Ltd.
      |545,686,ki,Kiribati,09,Kiribati Frigate
      |467,850,kp,Korea N. Dem. People's Rep.,193,Sun Net
      |450,82,kr,Korea S Republic of,02,KT Freetel Co. Ltd.
      |450,82,kr,Korea S Republic of,04,KT Freetel Co. Ltd.
      |450,82,kr,Korea S Republic of,08,KT Freetel Co. Ltd.
      |450,82,kr,Korea S Republic of,06,LG Telecom
      |450,82,kr,Korea S Republic of,03,SK Telecom
      |450,82,kr,Korea S Republic of,05,SK Telecom Co. Ltd
      |419,965,kw,Kuwait,04,Viva
      |419,965,kw,Kuwait,03,Wantaniya
      |419,965,kw,Kuwait,02,Zain
      |437,996,kg,Kyrgyzstan,03,AkTel LLC
      |437,996,kg,Kyrgyzstan,01,Beeline/Bitel
      |437,996,kg,Kyrgyzstan,05,MEGACOM
      |437,996,kg,Kyrgyzstan,09,O!/NUR Telecom
      |457,856,la,Laos P.D.R.,02,ETL Mobile
      |457,856,la,Laos P.D.R.,01,Lao Tel
      |457,856,la,Laos P.D.R.,08,Tigo/Millicom
      |457,856,la,Laos P.D.R.,03,UNITEL/LAT
      |247,371,lv,Latvia,05,Bite Latvija
      |247,371,lv,Latvia,01,Latvian Mobile Phone
      |247,371,lv,Latvia,09,SIA Camel Mobile
      |247,371,lv,Latvia,08,SIA IZZI
      |247,371,lv,Latvia,07,SIA Master Telecom
      |247,371,lv,Latvia,06,SIA Rigatta
      |247,371,lv,Latvia,02,Tele2
      |247,371,lv,Latvia,03,TRIATEL/Telekom Baltija
      |415,961,lb,Lebanon,33,Cellis
      |415,961,lb,Lebanon,32,Cellis
      |415,961,lb,Lebanon,35,Cellis
      |415,961,lb,Lebanon,34,FTML Cellis
      |415,961,lb,Lebanon,39,MIC2/LibanCell
      |415,961,lb,Lebanon,38,MIC2/LibanCell
      |415,961,lb,Lebanon,37,MIC2/LibanCell
      |415,961,lb,Lebanon,01,MIC1 (Alfa)
      |415,961,lb,Lebanon,03,MIC2/LibanCell
      |415,961,lb,Lebanon,36,MIC2/LibanCell
      |651,266,ls,Lesotho,02,Econet/Ezi-cel
      |651,266,ls,Lesotho,01,Vodacom Lesotho
      |618,231,lr,Liberia,07,Celcom
      |618,231,lr,Liberia,03,Celcom
      |618,231,lr,Liberia,04,Comium BVI
      |618,231,lr,Liberia,02,Libercell
      |618,231,lr,Liberia,20,LibTelco
      |618,231,lr,Liberia,01,Lonestar
      |606,218,ly,Libya,02,Al-Madar
      |606,218,ly,Libya,01,Al-Madar
      |606,218,ly,Libya,06,Hatef
      |606,218,ly,Libya,00,Libyana
      |606,218,ly,Libya,03,Libyana
      |295,423,li,Liechtenstein,06,CUBIC (Liechtenstein
      |295,423,li,Liechtenstein,07,First Mobile AG
      |295,423,li,Liechtenstein,05,Mobilkom AG
      |295,423,li,Liechtenstein,02,Orange
      |295,423,li,Liechtenstein,01,Swisscom FL AG
      |295,423,li,Liechtenstein,77,Alpmobile/Tele2
      |246,370,lt,Lithuania,02,Bite
      |246,370,lt,Lithuania,01,Omnitel
      |246,370,lt,Lithuania,03,Tele2
      |270,352,lu,Luxembourg,77,Millicom Tango GSM
      |270,352,lu,Luxembourg,01,P+T LUXGSM
      |270,352,lu,Luxembourg,99,VOXmobile S.A.
      |455,853,mo,Macao China,04,C.T.M. TELEMOVEL+
      |455,853,mo,Macao China,01,C.T.M. TELEMOVEL+
      |455,853,mo,Macao China,02,China Telecom
      |455,853,mo,Macao China,05,Hutchison Telephone (Macau) Company Ltd
      |455,853,mo,Macao China,03,Hutchison Telephone (Macau) Company Ltd
      |455,853,mo,Macao China,06,Smartone Mobile
      |455,853,mo,Macao China,00,Smartone Mobile
      |294,389,mk,Macedonia,75,MTS/Cosmofone
      |294,389,mk,Macedonia,02,MTS/Cosmofone
      |294,389,mk,Macedonia,01,T-Mobile/Mobimak
      |294,389,mk,Macedonia,03,VIP Mobile
      |646,261,mg,Madagascar,01,MADACOM
      |646,261,mg,Madagascar,02,Orange/Soci
      |646,261,mg,Madagascar,03,Sacel
      |646,261,mg,Madagascar,04,Telma
      |650,265,mw,Malawi,01,TNM/Telekom Network Ltd.
      |650,265,mw,Malawi,10,Zain/Celtel ltd.
      |502,60,my,Malaysia,01,Art900
      |502,60,my,Malaysia,151,Baraka Telecom Sdn Bhd
      |502,60,my,Malaysia,13,CelCom
      |502,60,my,Malaysia,19,CelCom
      |502,60,my,Malaysia,16,Digi Telecommunications
      |502,60,my,Malaysia,10,Digi Telecommunications
      |502,60,my,Malaysia,20,Electcoms Wireless Sdn Bhd
      |502,60,my,Malaysia,12,Maxis
      |502,60,my,Malaysia,17,Maxis
      |502,60,my,Malaysia,11,MTX Utara
      |502,60,my,Malaysia,153,Packet One Networks (Malaysia) Sdn Bhd
      |502,60,my,Malaysia,155,Samata Communications Sdn Bhd
      |502,60,my,Malaysia,154,Talk Focus Sdn Bhd
      |502,60,my,Malaysia,18,U Mobile
      |502,60,my,Malaysia,152,YES
      |472,960,mv,Maldives,01,Dhiraagu/C&W
      |472,960,mv,Maldives,02,Wataniya/WMOBILE
      |610,223,ml,Mali,01,Malitel
      |610,223,ml,Mali,02,Orange/IKATEL
      |278,356,mt,Malta,21,GO/Mobisle
      |278,356,mt,Malta,77,Melita
      |278,356,mt,Malta,01,Vodafone
      |340,596,mq,Martinique (French Department of),02,Outremer Telecom Martinique (only) (Martinique)
      |340,596,mq,Martinique (French Department of),12,United Telecommunications Services Caraibe SARL (UTS """ +
        """Caraibe Martinique Telephone Mobile) (Martinique)
      |340,596,mq,Martinique (French Department of),03,United Telecommunications Services Caraibe SARL (UTS """ +
        """Caraibe Martinique Telephone Mobile) (Martinique)
      |609,222,mr,Mauritania,02,Chinguitel SA
      |609,222,mr,Mauritania,01,Mattel
      |609,222,mr,Mauritania,10,Mauritel
      |617,230,mu,Mauritius,10,Emtel Ltd
      |617,230,mu,Mauritius,02,Mahanagar Telephone
      |617,230,mu,Mauritius,03,Mahanagar Telephone
      |617,230,mu,Mauritius,01,Orange/Cellplus
      |334,52,mx,Mexico,00,Axtel
      |334,52,mx,Mexico,50,IUSACell/UneFon
      |334,52,mx,Mexico,050,IUSACell/UneFon
      |334,52,mx,Mexico,040,IUSACell/UneFon
      |334,52,mx,Mexico,04,IUSACell/UneFon
      |334,52,mx,Mexico,03,Movistar/Pegaso
      |334,52,mx,Mexico,030,Movistar/Pegaso
      |334,52,mx,Mexico,01,NEXTEL
      |334,52,mx,Mexico,090,NEXTEL
      |334,52,mx,Mexico,010,NEXTEL
      |334,52,mx,Mexico,080,Operadora Unefon SA de CV
      |334,52,mx,Mexico,070,Operadora Unefon SA de CV
      |334,52,mx,Mexico,060,SAI PCS
      |334,52,mx,Mexico,00,SAI PCS
      |334,52,mx,Mexico,02,TelCel/America Movil
      |334,52,mx,Mexico,020,TelCel/America Movil
      |550,691,fm,Micronesia,01,FSM Telecom
      |259,373,md,Moldova,04,Eventis Mobile
      |259,373,md,Moldova,99,IDC/Unite
      |259,373,md,Moldova,05,IDC/Unite
      |259,373,md,Moldova,03,IDC/Unite
      |259,373,md,Moldova,02,Moldcell
      |259,373,md,Moldova,01,Orange/Voxtel
      |212,377,mc,Monaco,01,Dardafone LLC
      |212,377,mc,Monaco,10,Monaco Telecom
      |212,377,mc,Monaco,01,Monaco Telecom
      |212,377,mc,Monaco,01,Post and Telecommunications of Kosovo JSC (PTK)
      |428,976,mn,Mongolia,98,G-Mobile Corporation Ltd
      |428,976,mn,Mongolia,99,Mobicom
      |428,976,mn,Mongolia,00,Skytel Co. Ltd
      |428,976,mn,Mongolia,88,Unitel
      |297,382,me,Montenegro,02,Monet/T-mobile
      |297,382,me,Montenegro,03,Mtel
      |297,382,me,Montenegro,01,Promonte GSM
      |354,1664,ms,Montserrat,860,Cable & Wireless
      |604,212,ma,Morocco,01,IAM/Itissallat
      |604,212,ma,Morocco,02,INWI/WANA
      |604,212,ma,Morocco,00,Medi Telecom
      |643,258,mz,Mozambique,01,mCel
      |643,258,mz,Mozambique,03,Movitel
      |643,258,mz,Mozambique,04,Vodacom Sarl
      |649,264,na,Namibia,03,Leo / Orascom
      |649,264,na,Namibia,01,MTC
      |649,264,na,Namibia,02,Switch/Nam. Telec.
      |429,977,np,Nepal,02,Ncell
      |429,977,np,Nepal,01,NT Mobile / Namaste
      |429,977,np,Nepal,04,Smart Cell
      |204,31,nl,Netherlands,14,6GMOBILE BV
      |204,31,nl,Netherlands,23,Aspider Solutions
      |204,31,nl,Netherlands,05,Elephant Talk Communications Premium Rate Services Netherlands BV
      |204,31,nl,Netherlands,17,Intercity Mobile Communications BV
      |204,31,nl,Netherlands,10,KPN Telecom B.V.
      |204,31,nl,Netherlands,08,KPN Telecom B.V.
      |204,31,nl,Netherlands,69,KPN Telecom B.V.
      |204,31,nl,Netherlands,12,KPN/Telfort
      |204,31,nl,Netherlands,28,Lancelot BV
      |204,31,nl,Netherlands,09,Lycamobile Ltd
      |204,31,nl,Netherlands,06,Mundio/Vectone Mobile
      |204,31,nl,Netherlands,21,NS Railinfrabeheer B.V.
      |204,31,nl,Netherlands,24,Private Mobility Nederland BV
      |204,31,nl,Netherlands,98,T-Mobile B.V.
      |204,31,nl,Netherlands,16,T-Mobile B.V.
      |204,31,nl,Netherlands,20,Orange/T-mobile
      |204,31,nl,Netherlands,02,Tele2
      |204,31,nl,Netherlands,07,Teleena Holding BV
      |204,31,nl,Netherlands,68,Unify Mobile
      |204,31,nl,Netherlands,18,UPC Nederland BV
      |204,31,nl,Netherlands,04,Vodafone Libertel
      |204,31,nl,Netherlands,03,Voiceworks Mobile BV
      |204,31,nl,Netherlands,15,Ziggo BV
      |362,599,an,Netherlands Antilles,630,Cingular Wireless
      |362,599,an,Netherlands Antilles,69,DigiCell
      |362,599,an,Netherlands Antilles,51,TELCELL GSM
      |362,599,an,Netherlands Antilles,91,SETEL GSM
      |362,599,an,Netherlands Antilles,951,UTS Wireless
      |546,687,nc,New Caledonia,01,OPT Mobilis
      |530,64,nz,New Zealand,28,2degrees
      |530,64,nz,New Zealand,05,Telecom Mobile Ltd
      |530,64,nz,New Zealand,02,NZ Telecom CDMA
      |530,64,nz,New Zealand,04,Telstra
      |530,64,nz,New Zealand,24,Two Degrees Mobile Ltd
      |530,64,nz,New Zealand,01,Vodafone
      |530,64,nz,New Zealand,03,Walker Wireless Ltd.
      |710,505,ni,Nicaragua,21,Empresa Nicaraguense de Telecomunicaciones SA (ENITEL)
      |710,505,ni,Nicaragua,30,Movistar
      |710,505,ni,Nicaragua,73,Claro
      |614,227,ne,Niger,03,Etisalat/TeleCel
      |614,227,ne,Niger,04,Orange/Sahelc.
      |614,227,ne,Niger,01,Orange/Sahelc.
      |614,227,ne,Niger,02,Zain/CelTel
      |621,234,ng,Nigeria,20,Airtel/ZAIN/Econet
      |621,234,ng,Nigeria,60,ETISALAT
      |621,234,ng,Nigeria,50,Glo Mobile
      |621,234,ng,Nigeria,40,M-Tel/Nigeria Telecom. Ltd.
      |621,234,ng,Nigeria,30,MTN
      |621,234,ng,Nigeria,99,Starcomms
      |621,234,ng,Nigeria,25,Visafone
      |621,234,ng,Nigeria,01,Visafone
      |555,683,nu,Niue,01,Niue Telecom
      |242,47,no,Norway,09,Com4 AS
      |242,47,no,Norway,20,Jernbaneverket (GSM-R)
      |242,47,no,Norway,21,Jernbaneverket (GSM-R)
      |242,47,no,Norway,23,Lycamobile Ltd
      |242,47,no,Norway,02,Netcom
      |242,47,no,Norway,22,Network Norway AS
      |242,47,no,Norway,05,Network Norway AS
      |242,47,no,Norway,06,ICE Nordisk Mobiltelefon AS
      |242,47,no,Norway,08,TDC Mobil A/S
      |242,47,no,Norway,04,Tele2
      |242,47,no,Norway,12,Telenor
      |242,47,no,Norway,01,Telenor
      |242,47,no,Norway,03,Teletopia
      |242,47,no,Norway,07,Ventelo AS
      |422,968,om,Oman,03,Nawras
      |422,968,om,Oman,02,Oman Mobile/GTO
      |410,92,pk,Pakistan,08,Instaphone
      |410,92,pk,Pakistan,01,Mobilink
      |410,92,pk,Pakistan,06,Telenor
      |410,92,pk,Pakistan,03,UFONE/PAKTel
      |410,92,pk,Pakistan,07,Warid Telecom
      |410,92,pk,Pakistan,04,ZONG/CMPak
      |552,680,pw,Palau (Republic of),80,Palau Mobile Corp. (PMC) (Palau
      |552,680,pw,Palau (Republic of),01,Palau National Communications Corp. (PNCC) (Palau
      |425,970,ps,Palestinian Territory,05,Jawwal
      |425,970,ps,Palestinian Territory,06,Wataniya Mobile
      |714,507,pa,Panama,01,Cable & Wireless S.A.
      |714,507,pa,Panama,03,Claro
      |714,507,pa,Panama,04,Digicel
      |714,507,pa,Panama,020,Movistar
      |714,507,pa,Panama,02,Movistar
      |537,675,pg,Papua New Guinea,03,Digicel
      |537,675,pg,Papua New Guinea,02,GreenCom PNG Ltd
      |537,675,pg,Papua New Guinea,01,Pacific Mobile
      |744,595,py,Paraguay,02,Claro/Hutchison
      |744,595,py,Paraguay,03,Compa
      |744,595,py,Paraguay,01,Hola/VOX
      |744,595,py,Paraguay,05,TIM/Nucleo/Personal
      |744,595,py,Paraguay,04,Tigo/Telecel
      |716,51,pe,Peru,20,Claro /Amer.Mov./TIM
      |716,51,pe,Peru,10,Claro /Amer.Mov./TIM
      |716,51,pe,Peru,02,GlobalStar
      |716,51,pe,Peru,01,GlobalStar
      |716,51,pe,Peru,06,Movistar
      |716,51,pe,Peru,07,Nextel
      |515,63,ph,Philippines,00,Fix Line
      |515,63,ph,Philippines,01,Globe Telecom
      |515,63,ph,Philippines,02,Globe Telecom
      |515,63,ph,Philippines,88,Next Mobile
      |515,63,ph,Philippines,18,RED Mobile/Cure
      |515,63,ph,Philippines,03,Smart
      |515,63,ph,Philippines,05,SUN/Digitel
      |260,48,pl,Poland,17,Aero2 SP.
      |260,48,pl,Poland,18,AMD Telecom.
      |260,48,pl,Poland,38,CallFreedom Sp. z o.o.
      |260,48,pl,Poland,12,Cyfrowy POLSAT S.A.
      |260,48,pl,Poland,08,e-Telko
      |260,48,pl,Poland,09,Lycamobile
      |260,48,pl,Poland,16,Mobyland
      |260,48,pl,Poland,36,Mundio Mobile Sp. z o.o.
      |260,48,pl,Poland,07,Play/P4
      |260,48,pl,Poland,11,NORDISK Polska
      |260,48,pl,Poland,05,Orange/IDEA/Centertel
      |260,48,pl,Poland,03,Orange/IDEA/Centertel
      |260,48,pl,Poland,35,PKP Polskie Linie Kolejowe S.A.
      |260,48,pl,Poland,98,Play/P4
      |260,48,pl,Poland,06,Play/P4
      |260,48,pl,Poland,01,Polkomtel/Plus
      |260,48,pl,Poland,10,Sferia
      |260,48,pl,Poland,14,Sferia
      |260,48,pl,Poland,13,Sferia
      |260,48,pl,Poland,34,T-Mobile/ERA
      |260,48,pl,Poland,02,T-Mobile/ERA
      |260,48,pl,Poland,15,Tele2
      |260,48,pl,Poland,04,Tele2
      |268,351,pt,Portugal,04,CTT - Correios de Portugal SA
      |268,351,pt,Portugal,03,Optimus
      |268,351,pt,Portugal,07,Optimus
      |268,351,pt,Portugal,06,TMN
      |268,351,pt,Portugal,01,Vodafone
      |330,1,pr,Puerto Rico,11,Puerto Rico Telephone Company Inc. (PRTC)
      |427,974,qa,Qatar,01,Qtel
      |427,974,qa,Qatar,02,Vodafone
      |647,262,re,Reunion,00,Orange
      |647,262,re,Reunion,02,Outremer Telecom
      |647,262,re,Reunion,10,SFR
      |226,40,ro,Romania,03,Cosmote
      |226,40,ro,Romania,11,Enigma Systems
      |226,40,ro,Romania,10,Orange
      |226,40,ro,Romania,05,RCS&RDS Digi Mobile
      |226,40,ro,Romania,02,Romtelecom SA
      |226,40,ro,Romania,06,Telemobil/Zapp
      |226,40,ro,Romania,01,Vodafone
      |226,40,ro,Romania,04,Telemobil/Zapp
      |250,79,ru,Russian Federation,12,Baykal Westcom
      |250,79,ru,Russian Federation,28,Bee Line GSM
      |250,79,ru,Russian Federation,10,DTC/Don Telecom
      |250,79,ru,Russian Federation,20,JSC Rostov Cellular Communications
      |250,79,ru,Russian Federation,13,Kuban GSM
      |250,79,ru,Russian Federation,35,LLC Ekaterinburg-2000
      |250,79,ru,Russian Federation,20,LLC Personal Communication Systems in the Region
      |250,79,ru,Russian Federation,02,Megafon
      |250,79,ru,Russian Federation,01,MTS
      |250,79,ru,Russian Federation,03,NCC
      |250,79,ru,Russian Federation,16,NTC
      |250,79,ru,Russian Federation,19,OJSC Altaysvyaz
      |250,79,ru,Russian Federation,99,OJSC Vimpel-Communications (VimpelCom)
      |250,79,ru,Russian Federation,11,Orensot
      |250,79,ru,Russian Federation,92,Printelefone
      |250,79,ru,Russian Federation,04,Sibchallenge
      |250,79,ru,Russian Federation,44,StavTelesot
      |250,79,ru,Russian Federation,20,Tele2/ECC/Volgogr.
      |250,79,ru,Russian Federation,93,Telecom XXL
      |250,79,ru,Russian Federation,39,U-Tel/Ermak RMS
      |250,79,ru,Russian Federation,17,U-Tel/Ermak RMS
      |250,79,ru,Russian Federation,39,UralTel
      |250,79,ru,Russian Federation,17,UralTel
      |250,79,ru,Russian Federation,05,Yenisey Telecom
      |250,79,ru,Russian Federation,15,ZAO SMARTS
      |250,79,ru,Russian Federation,07,ZAO SMARTS"""

  private val countryTable2 =
    """635,250,rw,Rwanda,14,Airtel Rwanda Ltd
      |635,250,rw,Rwanda,10,MTN/Rwandacell
      |635,250,rw,Rwanda,13,TIGO
      |356,1869,kn,Saint Kitts and Nevis,110,Cable & Wireless
      |356,1869,kn,Saint Kitts and Nevis,50,Digicel
      |356,1869,kn,Saint Kitts and Nevis,70,UTS Cariglobe
      |358,1758,lc,Saint Lucia,110,Cable & Wireless
      |358,1758,lc,Saint Lucia,30,Cingular Wireless
      |358,1758,lc,Saint Lucia,50,Digicel (St Lucia) Limited
      |549,685,ws,Samoa,27,Samoatel Mobile
      |549,685,ws,Samoa,01,Telecom Samoa Cellular Ltd.
      |292,378,sm,San Marino,01,Prima Telecom
      |626,239,st,Sao Tome & Principe,01,CSTmovel
      |901,870,n/a,Satellite Networks,14,AeroMobile
      |901,870,n/a,Satellite Networks,11,InMarSAT
      |901,870,n/a,Satellite Networks,12,Maritime Communications Partner AS
      |901,870,n/a,Satellite Networks,05,Thuraya Satellite
      |420,966,sa,Saudi Arabia,07,Zain
      |420,966,sa,Saudi Arabia,03,Etihad/Etisalat/Mobily
      |420,966,sa,Saudi Arabia,01,STC/Al Jawal
      |420,966,sa,Saudi Arabia,04,Zain
      |608,221,sn,Senegal,03,Expresso/Sudatel
      |608,221,sn,Senegal,01,Orange/Sonatel
      |608,221,sn,Senegal,02,Sentel GSM
      |220,381,rs,Serbia,03,MTS/Telekom Srbija
      |220,381,rs,Serbia,02,Telenor/Mobtel
      |220,381,rs,Serbia,01,Telenor/Mobtel
      |220,381,rs,Serbia,05,VIP Mobile
      |633,248,sc,Seychelles,10,Airtel
      |633,248,sc,Seychelles,01,C&W
      |633,248,sc,Seychelles,02,Smartcom
      |619,232,sl,Sierra Leone,03,Africel
      |619,232,sl,Sierra Leone,05,Africel
      |619,232,sl,Sierra Leone,01,Zain/Celtel
      |619,232,sl,Sierra Leone,04,Comium
      |619,232,sl,Sierra Leone,02,Tigo/Millicom
      |619,232,sl,Sierra Leone,25,Mobitel
      |525,65,sg,Singapore,12,GRID Communications Pte Ltd
      |525,65,sg,Singapore,03,MobileOne Ltd
      |525,65,sg,Singapore,01,Singtel
      |525,65,sg,Singapore,07,Singtel
      |525,65,sg,Singapore,02,Singtel
      |525,65,sg,Singapore,06,Starhub
      |525,65,sg,Singapore,05,Starhub
      |362,1721,sx,Sint Maarten (Dutch part),51,TelCell NV (Sint Maarten
      |362,1721,sx,Sint Maarten (Dutch part),91,UTS St. Maarten (Sint Maarten
      |231,421,sk,Slovakia,06,O2
      |231,421,sk,Slovakia,01,Orange
      |231,421,sk,Slovakia,05,Orange
      |231,421,sk,Slovakia,15,Orange
      |231,421,sk,Slovakia,02,T-Mobile
      |231,421,sk,Slovakia,04,T-Mobile
      |231,421,sk,Slovakia,99,Zeleznice Slovenskej republiky (ZSR)
      |293,386,si,Slovenia,41,Dukagjini Telecommunications Sh.P.K.
      |293,386,si,Slovenia,41,Ipko Telecommunications d. o. o.
      |293,386,si,Slovenia,41,Mobitel
      |293,386,si,Slovenia,40,SI.Mobil
      |293,386,si,Slovenia,10,Slovenske zeleznice d.o.o.
      |293,386,si,Slovenia,64,T-2 d.o.o.
      |293,386,si,Slovenia,70,TusMobil/VEGA
      |540,677,sb,Solomon Islands,02,bemobile
      |540,677,sb,Solomon Islands,10,BREEZE
      |540,677,sb,Solomon Islands,01,BREEZE
      |637,252,so,Somalia,30,Golis
      |637,252,so,Somalia,19,HorTel
      |637,252,so,Somalia,60,Nationlink
      |637,252,so,Somalia,10,Nationlink
      |637,252,so,Somalia,04,Somafone
      |637,252,so,Somalia,82,Telcom Mobile Somalia
      |637,252,so,Somalia,01,Telesom
      |655,27,za,South Africa,02,8.ta
      |655,27,za,South Africa,21,Cape Town Metropolitan
      |655,27,za,South Africa,07,Cell C
      |655,27,za,South Africa,12,MTN
      |655,27,za,South Africa,10,MTN
      |655,27,za,South Africa,06,Sentech
      |655,27,za,South Africa,01,Vodacom
      |655,27,za,South Africa,19,Wireless Business Solutions (Pty) Ltd
      |659,211,ss,South Sudan (Republic of),03,Gemtel Ltd (South Sudan
      |659,211,ss,South Sudan (Republic of),02,MTN South Sudan (South Sudan
      |659,211,ss,South Sudan (Republic of),04,Network of The World Ltd (NOW) (South Sudan
      |659,211,ss,South Sudan (Republic of),06,Zain South Sudan (South Sudan
      |214,34,es,Spain,23,Lycamobile SL
      |214,34,es,Spain,22,Movistar
      |214,34,es,Spain,15,BT Espana Compania de Servicios Globales de Telecomunicaciones SAU
      |214,34,es,Spain,18,Cableuropa SAU (ONO)
      |214,34,es,Spain,08,Euskaltel SA
      |214,34,es,Spain,20,fonYou Wireless SL
      |214,34,es,Spain,21,Jazz Telecom SAU
      |214,34,es,Spain,26,Lleida
      |214,34,es,Spain,25,Lycamobile SL
      |214,34,es,Spain,07,Movistar2
      |214,34,es,Spain,05,Movistar
      |214,34,es,Spain,09,Orange
      |214,34,es,Spain,03,Orange2
      |214,34,es,Spain,11,Orange
      |214,34,es,Spain,17,R Cable y Telecomunicaciones Galicia SA
      |214,34,es,Spain,19,Simyo/KPN
      |214,34,es,Spain,16,Telecable de Asturias SA
      |214,34,es,Spain,27,Truphone
      |214,34,es,Spain,01,Vodafone
      |214,34,es,Spain,06,Vodafone Enabler Espana SL
      |214,34,es,Spain,04,Yoigo
      |413,94,lk,Sri Lanka,05,Bharti Airtel
      |413,94,lk,Sri Lanka,03,Etisalat/Tigo
      |413,94,lk,Sri Lanka,08,H3G Hutchison
      |413,94,lk,Sri Lanka,01,Mobitel Ltd.
      |413,94,lk,Sri Lanka,02,MTN/Dialog
      |308,508,pm,St. Pierre & Miquelon,01,Ameris
      |360,1784,vc,St. Vincent & Gren.,110,C & W
      |360,1784,vc,St. Vincent & Gren.,10,Cingular
      |360,1784,vc,St. Vincent & Gren.,100,Cingular
      |360,1784,vc,St. Vincent & Gren.,050,Digicel
      |360,1784,vc,St. Vincent & Gren.,70,Digicel
      |634,249,sd,Sudan,00,Canar Telecom
      |634,249,sd,Sudan,22,MTN
      |634,249,sd,Sudan,02,MTN
      |634,249,sd,Sudan,15,Sudani One
      |634,249,sd,Sudan,07,Sudani One
      |634,249,sd,Sudan,05,Vivacell
      |634,249,sd,Sudan,08,Vivacell
      |634,249,sd,Sudan,01,ZAIN/Mobitel
      |634,249,sd,Sudan,06,ZAIN/Mobitel
      |746,597,sr,Suriname,03,Digicel
      |746,597,sr,Suriname,02,Telecommunicatiebedrijf Suriname (TELESUR)
      |746,597,sr,Suriname,01,Telesur
      |746,597,sr,Suriname,04,UNIQA
      |653,268,sz,Swaziland,10,Swazi MTN
      |653,268,sz,Swaziland,01,SwaziTelecom
      |240,46,se,Sweden,35,42 Telecom AB
      |240,46,se,Sweden,16,42 Telecom AB
      |240,46,se,Sweden,26,Beepsend
      |240,46,se,Sweden,00,Compatel
      |240,46,se,Sweden,28,CoolTEL Aps
      |240,46,se,Sweden,25,Digitel Mobile Srl
      |240,46,se,Sweden,22,Eu Tel AB
      |240,46,se,Sweden,27,Fogg Mobile AB
      |240,46,se,Sweden,18,Generic Mobile Systems Sweden AB
      |240,46,se,Sweden,17,Gotalandsnatet AB
      |240,46,se,Sweden,02,H3G Access AB
      |240,46,se,Sweden,04,H3G Access AB
      |240,46,se,Sweden,36,ID Mobile
      |240,46,se,Sweden,23,Infobip Ltd.
      |240,46,se,Sweden,11,Lindholmen Science Park AB
      |240,46,se,Sweden,12,Lycamobile Ltd
      |240,46,se,Sweden,29,Mercury International Carrier Services
      |240,46,se,Sweden,03,Orange
      |240,46,se,Sweden,10,Spring Mobil AB
      |240,46,se,Sweden,14,TDC Sverige AB
      |240,46,se,Sweden,07,Tele2 Sverige AB
      |240,46,se,Sweden,05,Tele2 Sverige AB
      |240,46,se,Sweden,24,Tele2 Sverige AB
      |240,46,se,Sweden,24,Telenor (Vodafone)
      |240,46,se,Sweden,08,Telenor (Vodafone)
      |240,46,se,Sweden,04,Telenor (Vodafone)
      |240,46,se,Sweden,06,Telenor (Vodafone)
      |240,46,se,Sweden,09,Telenor Mobile Sverige AS
      |240,46,se,Sweden,05,Telia Mobile
      |240,46,se,Sweden,01,Telia Mobile
      |240,46,se,Sweden,00,EUTel
      |240,46,se,Sweden,08,Timepiece Servicos De Consultoria LDA (Universal Telecom)
      |240,46,se,Sweden,13,Ventelo Sverige AB
      |240,46,se,Sweden,20,Wireless Maingate AB
      |240,46,se,Sweden,15,Wireless Maingate Nordic AB
      |228,41,ch,Switzerland,51,BebbiCell AG
      |228,41,ch,Switzerland,09,Comfone AG
      |228,41,ch,Switzerland,05,Comfone AG
      |228,41,ch,Switzerland,07,TDC Sunrise
      |228,41,ch,Switzerland,54,Lycamobile AG
      |228,41,ch,Switzerland,52,Mundio Mobile AG
      |228,41,ch,Switzerland,03,Orange
      |228,41,ch,Switzerland,01,Swisscom
      |228,41,ch,Switzerland,12,TDC Sunrise
      |228,41,ch,Switzerland,02,TDC Sunrise
      |228,41,ch,Switzerland,08,TDC Sunrise
      |228,41,ch,Switzerland,53,upc cablecom GmbH
      |417,963,sy,Syrian Arab Republic,02,MTN/Spacetel
      |417,963,sy,Syrian Arab Republic,09,Syriatel Holdings
      |417,963,sy,Syrian Arab Republic,01,Syriatel Holdings
      |466,886,tw,Taiwan,68,ACeS Taiwan - ACeS Taiwan Telecommunications Co Ltd
      |466,886,tw,Taiwan,05,Asia Pacific Telecom Co. Ltd (APT)
      |466,886,tw,Taiwan,11,Chunghwa Telecom LDM
      |466,886,tw,Taiwan,92,Chunghwa Telecom LDM
      |466,886,tw,Taiwan,02,Far EasTone
      |466,886,tw,Taiwan,01,Far EasTone
      |466,886,tw,Taiwan,07,Far EasTone
      |466,886,tw,Taiwan,06,Far EasTone
      |466,886,tw,Taiwan,03,Far EasTone
      |466,886,tw,Taiwan,10,Global Mobile Corp.
      |466,886,tw,Taiwan,56,International Telecom Co. Ltd (FITEL)
      |466,886,tw,Taiwan,88,KG Telecom
      |466,886,tw,Taiwan,99,TransAsia
      |466,886,tw,Taiwan,97,Taiwan Cellular
      |466,886,tw,Taiwan,93,Mobitai
      |466,886,tw,Taiwan,89,VIBO
      |466,886,tw,Taiwan,09,VMAX Telecom Co. Ltd
      |436,992,tj,Tajikistan,04,Babilon-M
      |436,992,tj,Tajikistan,05,Bee Line
      |436,992,tj,Tajikistan,02,CJSC Indigo Tajikistan
      |436,992,tj,Tajikistan,12,Tcell/JC Somoncom
      |436,992,tj,Tajikistan,03,MLT/TT mobile
      |436,992,tj,Tajikistan,01,Tcell/JC Somoncom
      |640,255,tz,Tanzania,08,Benson Informatics Ltd
      |640,255,tz,Tanzania,06,Dovetel (T) Ltd
      |640,255,tz,Tanzania,09,ExcellentCom (T) Ltd
      |640,255,tz,Tanzania,11,Smile Communications Tanzania Ltd
      |640,255,tz,Tanzania,07,Tanzania Telecommunications Company Ltd (TTCL)
      |640,255,tz,Tanzania,02,TIGO/MIC
      |640,255,tz,Tanzania,01,Tri Telecomm. Ltd.
      |640,255,tz,Tanzania,04,Vodacom Ltd
      |640,255,tz,Tanzania,05,ZAIN/Celtel
      |640,255,tz,Tanzania,03,Zantel/Zanzibar Telecom
      |520,66,th,Thailand,20,ACeS Thailand - ACeS Regional Services Co Ltd
      |520,66,th,Thailand,15,ACT Mobile
      |520,66,th,Thailand,03,Advanced Wireless Networks/AWN
      |520,66,th,Thailand,01,AIS/Advanced Info Service
      |520,66,th,Thailand,23,Digital Phone Co.
      |520,66,th,Thailand,00,Hutch/CAT CDMA
      |520,66,th,Thailand,18,Total Access (DTAC)
      |520,66,th,Thailand,05,Total Access (DTAC)
      |520,66,th,Thailand,04,True Move/Orange
      |520,66,th,Thailand,99,True Move/Orange
      |514,670,tl,Timor-Leste,01,Telin/ Telkomcel
      |514,670,tl,Timor-Leste,02,Timor Telecom
      |615,228,tg,Togo,02,Telecel/MOOV
      |615,228,tg,Togo,03,Telecel/MOOV
      |615,228,tg,Togo,01,Togo Telecom/TogoCELL
      |539,676,to,Tonga,43,Shoreline Communication
      |539,676,to,Tonga,01,Tonga Communications
      |374,1868,tt,Trinidad and Tobago,129,Bmobile/TSTT
      |374,1868,tt,Trinidad and Tobago,130,Digicel
      |374,1868,tt,Trinidad and Tobago,140,LaqTel Ltd.
      |605,216,tn,Tunisia,01,Orange
      |605,216,tn,Tunisia,03,Orascom Telecom
      |605,216,tn,Tunisia,02,Tunisie Telecom
      |286,90,tr,Turkey,04,AVEA/Aria
      |286,90,tr,Turkey,03,AVEA/Aria
      |286,90,tr,Turkey,01,Turkcell
      |286,90,tr,Turkey,02,Vodafone-Telsim
      |438,993,tm,Turkmenistan,01,Barash Communication
      |438,993,tm,Turkmenistan,02,TM-Cell
      |376,1649,tc,Turks and Caicos Islands,350,Cable & Wireless (TCI) Ltd
      |376,1649,tc,Turks and Caicos Islands,050,Digicel TCI Ltd
      |376,1649,tc,Turks and Caicos Islands,352,IslandCom Communications Ltd.
      |553,688,tv,Tuvalu,01,Tuvalu Telecommunication Corporation (TTC)
      |641,256,ug,Uganda,01,Celtel
      |641,256,ug,Uganda,66,i-Tel Ltd
      |641,256,ug,Uganda,30,K2 Telecom Ltd
      |641,256,ug,Uganda,10,MTN Ltd.
      |641,256,ug,Uganda,14,Orange
      |641,256,ug,Uganda,33,Smile Communications Uganda Ltd
      |641,256,ug,Uganda,18,Suretelecom Uganda Ltd
      |641,256,ug,Uganda,11,Uganda Telecom Ltd.
      |641,256,ug,Uganda,22,Airtel/Warid
      |255,380,ua,Ukraine,06,Astelit/LIFE
      |255,380,ua,Ukraine,05,Golden Telecom
      |255,380,ua,Ukraine,39,Golden Telecom
      |255,380,ua,Ukraine,04,Intertelecom Ltd (IT)
      |255,380,ua,Ukraine,67,KyivStar
      |255,380,ua,Ukraine,03,KyivStar
      |255,380,ua,Ukraine,21,Telesystems Of Ukraine CJSC (TSU)
      |255,380,ua,Ukraine,07,TriMob LLC
      |255,380,ua,Ukraine,50,UMC/MTS
      |255,380,ua,Ukraine,02,Beeline
      |255,380,ua,Ukraine,01,UMC/MTS
      |255,380,ua,Ukraine,68,Beeline
      |424,971,ae,United Arab Emirates,03,DU
      |431,971,ae,United Arab Emirates,02,Etisalat
      |430,971,ae,United Arab Emirates,02,Etisalat
      |424,971,ae,United Arab Emirates,02,Etisalat
      |234,44,gb,United Kingdom,03,Airtel/Vodafone
      |234,44,gb,United Kingdom,76,BT Group
      |234,44,gb,United Kingdom,77,BT Group
      |234,44,gb,United Kingdom,07,Cable and Wireless
      |234,44,gb,United Kingdom,92,Cable and Wireless
      |234,44,gb,United Kingdom,36,Calbe and Wireless Isle of Man
      |234,44,gb,United Kingdom,18,Cloud9/wire9 Tel.
      |235,44,gb,United Kingdom,02,Everyth. Ev.wh.
      |234,44,gb,United Kingdom,17,FlexTel
      |234,44,gb,United Kingdom,55,Guernsey Telecoms
      |234,44,gb,United Kingdom,14,HaySystems
      |234,44,gb,United Kingdom,94,Hutchinson 3G
      |234,44,gb,United Kingdom,20,Hutchinson 3G
      |234,44,gb,United Kingdom,75,Inquam Telecom Ltd
      |234,44,gb,United Kingdom,50,Jersey Telecom
      |234,44,gb,United Kingdom,35,JSC Ingenicum
      |234,44,gb,United Kingdom,26,Lycamobile
      |234,44,gb,United Kingdom,58,Manx Telecom
      |234,44,gb,United Kingdom,01,Mapesbury C. Ltd
      |234,44,gb,United Kingdom,28,Marthon Telecom
      |234,44,gb,United Kingdom,10,O2 Ltd.
      |234,44,gb,United Kingdom,02,O2 Ltd.
      |234,44,gb,United Kingdom,11,O2 Ltd.
      |234,44,gb,United Kingdom,08,OnePhone
      |234,44,gb,United Kingdom,16,Opal Telecom
      |234,44,gb,United Kingdom,34,Everyth. Ev.wh./Orange
      |234,44,gb,United Kingdom,33,Everyth. Ev.wh./Orange
      |234,44,gb,United Kingdom,19,PMN/Teleware
      |234,44,gb,United Kingdom,12,Railtrack Plc
      |234,44,gb,United Kingdom,22,Routotelecom
      |234,44,gb,United Kingdom,24,Stour Marine
      |234,44,gb,United Kingdom,37,Synectiv Ltd.
      |234,44,gb,United Kingdom,31,Everyth. Ev.wh./T-Mobile
      |234,44,gb,United Kingdom,30,Everyth. Ev.wh./T-Mobile
      |234,44,gb,United Kingdom,32,Everyth. Ev.wh./T-Mobile
      |234,44,gb,United Kingdom,27,Vodafone
      |234,44,gb,United Kingdom,09,Tismi
      |234,44,gb,United Kingdom,25,Truphone
      |234,44,gb,United Kingdom,51,Jersey Telecom
      |234,44,gb,United Kingdom,23,Vectofone Mobile Wifi
      |234,44,gb,United Kingdom,15,Vodafone
      |234,44,gb,United Kingdom,91,Vodafone
      |234,44,gb,United Kingdom,78,Wave Telecom Ltd
      |310,1,us,United States,050,
      |310,1,us,United States,880,
      |310,1,us,United States,850,Aeris Comm. Inc.
      |310,1,us,United States,640,
      |310,1,us,United States,510,Airtel Wireless LLC
      |310,1,us,United States,190,Unknown
      |312,1,us,United States,090,Allied Wireless Communications Corporation
      |311,1,us,United States,130,
      |311,1,us,United States,030,Americell PA3 LP
      |310,1,us,United States,710,Arctic Slope Telephone Association Cooperative Inc.
      |310,1,us,United States,380,AT&T Wireless Inc.
      |310,1,us,United States,170,AT&T Wireless Inc.
      |310,1,us,United States,150,AT&T Wireless Inc.
      |310,1,us,United States,680,AT&T Wireless Inc.
      |310,1,us,United States,070,AT&T Wireless Inc.
      |310,1,us,United States,560,AT&T Wireless Inc.
      |310,1,us,United States,410,AT&T Wireless Inc.
      |310,1,us,United States,980,AT&T Wireless Inc.
      |311,1,us,United States,440,Bluegrass Wireless LLC
      |311,1,us,United States,810,Bluegrass Wireless LLC
      |311,1,us,United States,800,Bluegrass Wireless LLC
      |310,1,us,United States,900,Cable & Communications Corp.
      |311,1,us,United States,590,California RSA No. 3 Limited Partnership
      |311,1,us,United States,500,Cambridge Telephone Company Inc.
      |310,1,us,United States,830,Caprock Cellular Ltd.
      |311,1,us,United States,272,Verizon Wireless
      |311,1,us,United States,288,Verizon Wireless
      |311,1,us,United States,277,Verizon Wireless
      |311,1,us,United States,482,Verizon Wireless
      |310,1,us,United States,590,Verizon Wireless
      |311,1,us,United States,282,Verizon Wireless
      |311,1,us,United States,487,Verizon Wireless
      |311,1,us,United States,271,Verizon Wireless
      |311,1,us,United States,287,Verizon Wireless
      |311,1,us,United States,276,Verizon Wireless
      |311,1,us,United States,481,Verizon Wireless
      |310,1,us,United States,013,Verizon Wireless
      |311,1,us,United States,281,Verizon Wireless
      |311,1,us,United States,486,Verizon Wireless
      |311,1,us,United States,270,Verizon Wireless
      |311,1,us,United States,286,Verizon Wireless
      |311,1,us,United States,275,Verizon Wireless
      |311,1,us,United States,480,Verizon Wireless
      |310,1,us,United States,012,Verizon Wireless
      |311,1,us,United States,280,Verizon Wireless
      |311,1,us,United States,485,Verizon Wireless
      |311,1,us,United States,110,Verizon Wireless
      |311,1,us,United States,285,Verizon Wireless
      |311,1,us,United States,274,Verizon Wireless
      |311,1,us,United States,390,Verizon Wireless
      |310,1,us,United States,010,Verizon Wireless
      |311,1,us,United States,279,Verizon Wireless
      |311,1,us,United States,484,Verizon Wireless
      |310,1,us,United States,910,Verizon Wireless
      |311,1,us,United States,284,Verizon Wireless
      |311,1,us,United States,489,Verizon Wireless
      |311,1,us,United States,273,Verizon Wireless
      |311,1,us,United States,289,Verizon Wireless
      |310,1,us,United States,004,Verizon Wireless
      |311,1,us,United States,278,Verizon Wireless
      |311,1,us,United States,483,Verizon Wireless
      |310,1,us,United States,890,Verizon Wireless
      |311,1,us,United States,283,Verizon Wireless
      |311,1,us,United States,488,Verizon Wireless
      |312,1,us,United States,280,Cellular Network Partnership LLC
      |312,1,us,United States,270,Cellular Network Partnership LLC
      |310,1,us,United States,360,Cellular Network Partnership LLC
      |311,1,us,United States,190,
      |310,1,us,United States,230,Cellular South Licenses Inc.
      |310,1,us,United States,030,
      |311,1,us,United States,120,Choice Phone LLC
      |310,1,us,United States,480,Choice Phone LLC
      |310,1,us,United States,630,
      |310,1,us,United States,420,Cincinnati Bell Wireless LLC
      |310,1,us,United States,180,Cingular Wireless
      |310,1,us,United States,620,Coleman County Telco /Trans TX
      |311,1,us,United States,040,
      |310,1,us,United States,060,Consolidated Telcom
      |312,1,us,United States,380,
      |310,1,us,United States,930,
      |311,1,us,United States,240,
      |310,1,us,United States,080,
      |310,1,us,United States,700,Cross Valliant Cellular Partnership
      |311,1,us,United States,140,Cross Wireless Telephone Co.
      |312,1,us,United States,030,Cross Wireless Telephone Co.
      |311,1,us,United States,520,
      |311,1,us,United States,810,Cumberland Cellular Partnership
      |311,1,us,United States,800,Cumberland Cellular Partnership
      |311,1,us,United States,440,Cumberland Cellular Partnership
      |312,1,us,United States,040,Custer Telephone Cooperative Inc.
      |310,1,us,United States,016,Denali Spectrum License LLC
      |310,1,us,United States,440,Dobson Cellular Systems
      |310,1,us,United States,990,E.N.M.R. Telephone Coop.
      |312,1,us,United States,130,East Kentucky Network LLC
      |312,1,us,United States,120,East Kentucky Network LLC
      |310,1,us,United States,750,East Kentucky Network LLC
      |310,1,us,United States,090,Edge Wireless LLC
      |310,1,us,United States,610,Elkhart TelCo. / Epic Touch Co.
      |311,1,us,United States,210,
      |311,1,us,United States,311,Farmers
      |311,1,us,United States,460,Fisher Wireless Services Inc.
      |311,1,us,United States,370,GCI Communication Corp.
      |310,1,us,United States,430,GCI Communication Corp.
      |310,1,us,United States,920,Get Mobile Inc.
      |310,1,us,United States,970,
      |310,1,us,United States,007,Unknown
      |311,1,us,United States,250,i CAN_GSM
      |311,1,us,United States,340,Illinois Valley Cellular RSA 2 Partnership
      |311,1,us,United States,030,
      |312,1,us,United States,170,Iowa RSA No. 2 Limited Partnership
      |311,1,us,United States,410,Iowa RSA No. 2 Limited Partnership
      |310,1,us,United States,770,Iowa Wireless Services LLC
      |310,1,us,United States,650,Jasper
      |310,1,us,United States,870,Kaplan Telephone Company Inc.
      |311,1,us,United States,810,Kentucky RSA #3 Cellular General Partnership
      |311,1,us,United States,800,Kentucky RSA #3 Cellular General Partnership
      |311,1,us,United States,440,Kentucky RSA #3 Cellular General Partnership
      |311,1,us,United States,440,Kentucky RSA #4 Cellular General Partnership
      |311,1,us,United States,810,Kentucky RSA #4 Cellular General Partnership
      |311,1,us,United States,800,Kentucky RSA #4 Cellular General Partnership
      |312,1,us,United States,180,Keystone Wireless LLC
      |310,1,us,United States,690,Keystone Wireless LLC
      |311,1,us,United States,310,Lamar County Cellular
      |310,1,us,United States,016,LCW Wireless Operations LLC
      |310,1,us,United States,016,Leap Wireless International Inc.
      |311,1,us,United States,090,
      |310,1,us,United States,040,Matanuska Tel. Assn. Inc.
      |310,1,us,United States,780,Message Express Co. / Airlink PCS
      |311,1,us,United States,660,
      |311,1,us,United States,330,Michigan Wireless LLC
      |311,1,us,United States,000,
      |311,1,us,United States,390,
      |310,1,us,United States,400,Minnesota South. Wirel. Co. / Hickory
      |312,1,us,United States,010,Missouri RSA No 5 Partnership
      |311,1,us,United States,920,Missouri RSA No 5 Partnership
      |311,1,us,United States,020,Missouri RSA No 5 Partnership
      |311,1,us,United States,010,Missouri RSA No 5 Partnership
      |312,1,us,United States,220,Missouri RSA No 5 Partnership
      |310,1,us,United States,350,Mohave Cellular LP
      |310,1,us,United States,570,MTPCS LLC
      |310,1,us,United States,290,NEP Cellcorp Inc.
      |310,1,us,United States,034,Nevada Wireless LLC
      |311,1,us,United States,380,
      |310,1,us,United States,600,New-Cell Inc.
      |311,1,us,United States,100,
      |311,1,us,United States,300,Nexus Communications Inc.
      |310,1,us,United States,130,North Carolina RSA 3 Cellular Tel. Co.
      |312,1,us,United States,230,North Dakota Network Company
      |311,1,us,United States,610,North Dakota Network Company
      |310,1,us,United States,450,Northeast Colorado Cellular Inc.
      |311,1,us,United States,710,Northeast Wireless Networks LLC
      |310,1,us,United States,670,Northstar
      |310,1,us,United States,011,Northstar
      |311,1,us,United States,420,Northwest Missouri Cellular Limited Partnership
      |310,1,us,United States,540,
      |310,1,us,United States,740,
      |310,1,us,United States,760,Panhandle Telephone Cooperative Inc.
      |310,1,us,United States,580,PCS ONE
      |311,1,us,United States,170,PetroCom
      |311,1,us,United States,670,Pine Belt Cellular Inc.
      |311,1,us,United States,080,
      |310,1,us,United States,790,
      |310,1,us,United States,100,Plateau Telecommunications Inc.
      |310,1,us,United States,940,Poka Lambro Telco Ltd.
      |311,1,us,United States,540,
      |311,1,us,United States,730,
      |310,1,us,United States,500,Public Service Cellular Inc.
      |312,1,us,United States,160,RSA 1 Limited Partnership
      |311,1,us,United States,430,RSA 1 Limited Partnership
      |311,1,us,United States,350,Sagebrush Cellular Inc.
      |311,1,us,United States,030,Sagir Inc.
      |311,1,us,United States,910,
      |310,1,us,United States,046,SIMMETRY
      |311,1,us,United States,260,SLO Cellular Inc / Cellular One of San Luis
      |310,1,us,United States,320,Smith Bagley Inc.
      |310,1,us,United States,15,Unknown
      |316,1,us,United States,011,Southern Communications Services Inc.
      |312,1,us,United States,190,Sprint Spectrum
      |311,1,us,United States,880,Sprint Spectrum
      |311,1,us,United States,870,Sprint Spectrum
      |311,1,us,United States,490,Sprint Spectrum
      |310,1,us,United States,120,Sprint Spectrum
      |316,1,us,United States,010,Sprint Spectrum
      |310,1,us,United States,310,T-Mobile
      |310,1,us,United States,220,T-Mobile
      |310,1,us,United States,270,T-Mobile
      |310,1,us,United States,210,T-Mobile
      |310,1,us,United States,260,T-Mobile
      |310,1,us,United States,200,T-Mobile
      |310,1,us,United States,250,T-Mobile
      |310,1,us,United States,160,T-Mobile
      |310,1,us,United States,240,T-Mobile
      |310,1,us,United States,660,T-Mobile
      |310,1,us,United States,230,T-Mobile
      |310,1,us,United States,300,T-Mobile
      |310,1,us,United States,280,T-Mobile
      |310,1,us,United States,330,T-Mobile
      |310,1,us,United States,800,T-Mobile
      |310,1,us,United States,310,T-Mobile
      |311,1,us,United States,740,
      |310,1,us,United States,740,Telemetrix Inc.
      |310,1,us,United States,014,Testing
      |310,1,us,United States,950,Unknown
      |310,1,us,United States,860,Texas RSA 15B2 Limited Partnership
      |311,1,us,United States,830,Thumb Cellular Limited Partnership
      |311,1,us,United States,050,Thumb Cellular Limited Partnership
      |310,1,us,United States,460,TMP Corporation
      |310,1,us,United States,490,Triton PCS
      |312,1,us,United States,290,Uintah Basin Electronics Telecommunications Inc.
      |311,1,us,United States,860,Uintah Basin Electronics Telecommunications Inc.
      |310,1,us,United States,960,Uintah Basin Electronics Telecommunications Inc.
      |310,1,us,United States,020,Union Telephone Co.
      |311,1,us,United States,220,United States Cellular Corp.
      |310,1,us,United States,730,United States Cellular Corp.
      |311,1,us,United States,650,United Wireless Communications Inc.
      |310,1,us,United States,380,USA 3650 AT&T
      |310,1,us,United States,520,VeriSign
      |310,1,us,United States,003,Unknown
      |310,1,us,United States,230,Unknown
      |310,1,us,United States,240,Unknown
      |310,1,us,United States,250,Unknown
      |310,1,us,United States,530,West Virginia Wireless
      |310,1,us,United States,260,Unknown
      |310,1,us,United States,340,Westlink Communications LLC
      |311,1,us,United States,150,
      |311,1,us,United States,070,Wisconsin RSA #7 Limited Partnership
      |310,1,us,United States,390,Yorkville Telephone Cooperative
      |748,598,uy,Uruguay,01,Ancel/Antel
      |748,598,uy,Uruguay,03,Ancel/Antel
      |748,598,uy,Uruguay,10,Claro/AM Wireless
      |748,598,uy,Uruguay,07,MOVISTAR
      |434,998,uz,Uzbekistan,04,Bee Line/Unitel
      |434,998,uz,Uzbekistan,01,Buztel
      |434,998,uz,Uzbekistan,07,MTS/Uzdunrobita
      |434,998,uz,Uzbekistan,05,Ucell/Coscom
      |434,998,uz,Uzbekistan,02,Uzmacom
      |541,678,vu,Vanuatu,05,DigiCel
      |541,678,vu,Vanuatu,00,DigiCel
      |541,678,vu,Vanuatu,01,SMILE
      |734,58,ve,Venezuela,03,DigiTel C.A.
      |734,58,ve,Venezuela,02,DigiTel C.A.
      |734,58,ve,Venezuela,01,DigiTel C.A.
      |734,58,ve,Venezuela,06,Movilnet C.A.
      |734,58,ve,Venezuela,04,Movistar/TelCel
      |452,84,vn,Viet Nam,07,Beeline
      |452,84,vn,Viet Nam,01,Mobifone
      |452,84,vn,Viet Nam,03,S-Fone/Telecom
      |452,84,vn,Viet Nam,05,VietnaMobile
      |452,84,vn,Viet Nam,08,Viettel Mobile
      |452,84,vn,Viet Nam,06,Viettel Mobile
      |452,84,vn,Viet Nam,04,Viettel Mobile
      |452,84,vn,Viet Nam,02,Vinaphone
      |376,1340,vi,Virgin Islands U.S.,350,Cable & Wireless (Turks & Caicos)
      |376,1340,vi,Virgin Islands U.S.,50,Digicel
      |376,1340,vi,Virgin Islands U.S.,352,IslandCom
      |421,967,ye,Yemen,04,HITS/Y Unitel
      |421,967,ye,Yemen,02,MTN/Spacetel
      |421,967,ye,Yemen,01,Sabaphone
      |421,967,ye,Yemen,03,Yemen Mob. CDMA
      |645,260,zm,Zambia,03,Cell Z/MTS
      |645,260,zm,Zambia,02,MTN/Telecel
      |645,260,zm,Zambia,01,Zain/Celtel
      |648,263,zw,Zimbabwe,04,Econet
      |648,263,zw,Zimbabwe,01,Net One
      |648,263,zw,Zimbabwe,03,Telecel"""
}
// scalastyle:ignore file.size.limit
