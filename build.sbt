name := "edm-core"

version in ThisBuild := "0.1.0"

organization in ThisBuild := "sa.com.mobily"

scalaVersion in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0" % "compile,test,it,provided",
  "com.github.nscala-time" %% "nscala-time" % "1.4.0",
  "org.scalatest" %% "scalatest" % "2.0" % "test,it"
)
