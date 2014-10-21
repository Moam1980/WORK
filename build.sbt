name := "edm-core"

version in ThisBuild := "0.1.0"

organization in ThisBuild := "sa.com.mobily"

scalaVersion in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0" % "compile,test,it,provided",
  "org.apache.spark" %% "spark-sql" % "1.1.0" % "compile,test,it,provided",
  "com.twitter" % "parquet-avro" % "1.6.0rc3" % "compile,test,it",
  "org.apache.avro" % "avro" % "1.7.7" % "compile,test,it",
  "org.apache.avro" % "avro-mapred" % "1.7.7" % "compile,test,it",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "compile,test,it" exclude("javax.servlet", "servlet-api"),
  "org.scalatest" %% "scalatest" % "2.0" % "test,it"
)
