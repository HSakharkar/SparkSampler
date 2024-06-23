import sbt.nio.file.PathFilter
import sbtassembly.Assembly.{Library, Project}
import sbtassembly.AssemblyPlugin
ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("HSakharkar", "sparksampler", "130132023+HSakharkar@users.noreply.github.com"))

resolvers += Resolver.mavenLocal
resolvers += Resolver.url("sparkSamplerRepo", url("https://maven.pkg.github.com/HSakharkar"))

assemblyOption in assembly := (assemblyOption in assembly).value.withIncludeScala(true)

lazy val root = (project in file("."))
  .settings(
    name := "sparksampler",
      assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter {
        _.data.getName == "commons-logging-1.2.jar"
        }
      }
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql-api_2.12" % "3.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.12" % "3.5.1" % "provided"
libraryDependencies += "com.eed3si9n" % "sbt-assembly_2.12_1.0" % "2.2.0" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.18" % "test"

ThisBuild / versionScheme := Some("early-semver")

publishTo := sonatypePublishToBundle.value
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "HSakharkar",
  System.getenv("GH_TOKEN")
)

homepage := Some(url("https://github.com/HSakharkar/SparkSampler"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/HSakharkar/SparkSampler"),
    "scm:git@github.com:HSakharkar/SparkSampler.git"
  )
)
developers := List(
  Developer(id="HSakharkar", name="Hemant Sakharkar", email="130132023+HSakharkar@users.noreply.github.com", url=url("https://www.linkedin.com/in/hemantsakharkar/"))
)

/*publishTo := {
  val pkg = "https://maven.pkg.github.com/"
  if (isSnapshot.value)
    Some("snapshots" at pkg + "hsakharkar/repositories/snapshots")
  else
    Some("releases"  at pkg + "hsakharkar/repositories/releases")
}*/

ThisBuild / assemblyMergeStrategy := {
  case PathList("module-info.class",xs @ _*)          => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)

}
