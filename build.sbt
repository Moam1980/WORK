name := "edm-core"

version in ThisBuild := "0.7.0"

organization in ThisBuild := "sa.com.mobily"

scalaVersion in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

addCommandAlias("test-common", ";project edm-core-common ;test ;project /")

addCommandAlias("test-usercentric", ";project edm-core-usercentric ;test ;project /")

addCommandAlias("test-customer", ";project edm-core-customer ;test ;project /")

addCommandAlias("test-complete", ";project edm-core-complete ;test ;project /")

addCommandAlias("test-all", ";test-common ;test-usercentric ;test-customer ;test-complete ;project /")

addCommandAlias("scoverage-common", ";project edm-core-common ;scoverage:test ;project /")

addCommandAlias("scoverage-usercentric", ";project edm-core-usercentric ;scoverage:test ;project /")

addCommandAlias("scoverage-customer", ";project edm-core-customer ;scoverage:test ;project /")

addCommandAlias("scoverage-complete", ";project edm-core-complete ;scoverage:test ;project /")

addCommandAlias("scoverage-all", ";scoverage-common ;scoverage-usercentric ;scoverage-customer ;scoverage-complete ;project /")

addCommandAlias("sanity", ";clean ;compile ;scalastyle ;scoverage-all ;assembly")

libraryDependencies in ThisBuild ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "1.4.0",
  "org.apache.spark" %% "spark-core" % "1.1.1" % "compile,test,it,provided",
  "org.apache.spark" %% "spark-sql" % "1.1.1" % "compile,test,it,provided",
  "org.apache.spark" %% "spark-mllib" % "1.1.1" % "compile,test,it,provided",
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
