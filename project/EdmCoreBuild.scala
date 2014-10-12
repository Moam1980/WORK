/*
 * TODO: License goes here!
 */

import sbt._
import Keys._
import org.scalastyle.sbt._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{PathList, MergeStrategy}
import sbtunidoc.Plugin._
import scoverage._
import scoverage.ScoverageSbtPlugin._

object EdmCoreBuild extends Build {

  def projectId(name: String) = s"edm-core-$name"

  val edmCoreMergeStrategy = mergeStrategy in assembly := {
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf/services/.*") => MergeStrategy.concat
    case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
    case _ => MergeStrategy.first
  }

  lazy val testParallelSettings = Seq(parallelExecution in ScoverageTest := false, parallelExecution in Test := false)

  lazy val edmCoreAssembly = assemblySettings ++ edmCoreMergeStrategy
  lazy val AcceptanceTest = config("acceptance") extend(Test)

  lazy val root = (Project(id = projectId("root"), base = file("."))
    settings(unidocSettings: _*)
    settings(ScoverageSbtPlugin.instrumentSettings: _*)
    configs(IntegrationTest)
    settings(Defaults.itSettings: _*)
    settings(testParallelSettings: _*)
    aggregate(complete, common, model)
    )

  lazy val common = (Project(id = projectId("common"), base = file("common"))
    settings(ScoverageSbtPlugin.instrumentSettings: _*)
    settings(ScalastylePlugin.Settings: _*)
    configs(IntegrationTest)
    settings(Defaults.itSettings : _*)
    settings(testParallelSettings: _*)
    )

  lazy val model = (Project(id = projectId("model"), base = file("model"))
    settings(ScoverageSbtPlugin.instrumentSettings: _*)
    configs(IntegrationTest)
    settings(Defaults.itSettings : _*)
    settings(testParallelSettings: _*)
    dependsOn(common % "compile->compile;test->test")
    )

  lazy val complete = (
    Project(
      id = projectId("complete"),
      base = file("complete"),
      settings = Defaults.defaultSettings ++ sbtassembly.Plugin.assemblySettings ++
        addArtifact(Artifact(projectId("complete"),"assembly"), sbtassembly.Plugin.AssemblyKeys.assembly))
    settings(ScoverageSbtPlugin.instrumentSettings: _*)
    configs(IntegrationTest)
    settings(edmCoreAssembly: _*)
    settings(Defaults.itSettings: _*)
    settings(testParallelSettings: _*)
    dependsOn(common % "compile->compile;test->test")
    dependsOn(model % "compile->compile;test->test")
    )
}
