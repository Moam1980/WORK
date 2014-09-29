name := "edm-core"

version in ThisBuild := "0.1.0"

organization in ThisBuild := "sa.com.mobily"

scalaVersion in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

resolvers in ThisBuild ++= Seq(
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
)

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0" % "compile,test,it,provided",
  "org.geotools" % "gt-main" % "12.0",
  "org.geotools" % "gt-epsg-hsql" % "12.0",
  "com.vividsolutions" % "jts" % "1.13",
  "org.scalatest" %% "scalatest" % "2.0" % "test,it"
)
