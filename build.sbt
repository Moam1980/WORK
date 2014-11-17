name := "edm-core"

version in ThisBuild := "0.2.0"

organization in ThisBuild := "sa.com.mobily"

scalaVersion in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

addCommandAlias("testEdmCommon", ";project edm-core-common ;test ;project /")

addCommandAlias("testEdmUsercentric", ";project edm-core-usercentric ;test ;project /")

addCommandAlias("testEdmComplete", ";project edm-core-complete ;test ;project /")

addCommandAlias("testEdmAll", ";testEdmCommon ;testEdmUsercentric ;testEdmComplete ;project /")

addCommandAlias("scoverageEdmCommon", ";project edm-core-common ;scoverage:test ;project /")

addCommandAlias("scoverageEdmUsercentric", ";project edm-core-usercentric ;scoverage:test ;project /")

addCommandAlias("scoverageEdmComplete", ";project edm-core-complete ;scoverage:test ;project /")

addCommandAlias("scoverageEdmAll", ";scoverageEdmCommon ;scoverageEdmUsercentric ;scoverageEdmComplete ;project /")

addCommandAlias("sanity", ";clean ;compile ;scalastyle ;scoverageEdmAll ;assembly")

libraryDependencies in ThisBuild ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "1.4.0",
  "org.apache.spark" %% "spark-core" % "1.1.0" % "compile,test,it,provided",
  "org.apache.spark" %% "spark-sql" % "1.1.0" % "compile,test,it,provided",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "compile,test,it" exclude("javax.servlet", "servlet-api"),
  "org.scalatest" %% "scalatest" % "2.0" % "test,it"
)

publishTo in ThisBuild := {
  val nexus = "http://10.64.246.161/nexus/"
  Some("edm-core" at nexus + "content/repositories/edm-core-ci")
}

publishArtifact in (ThisBuild, packageDoc) := true

publishArtifact in (ThisBuild, packageSrc) := false

credentials in ThisBuild += Credentials(Path.userHome / ".ivy2" / ".credentials")
