resolvers in ThisBuild ++= Seq(
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
)

libraryDependencies in ThisBuild ++= Seq(
  "org.geotools" % "gt-main" % "10.4",
  "org.geotools" % "gt-epsg-hsql" % "10.4",
  "com.vividsolutions" % "jts" % "1.12",
  "au.com.bytecode" % "opencsv" % "2.4",
  "com.googlecode.libphonenumber" % "libphonenumber" % "6.3.1",
  "com.github.wookietreiber" % "scala-chart_2.10" % "0.4.2"
)
