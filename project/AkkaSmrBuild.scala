import sbt._
import Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

object AkkaSmrBuild extends Build {

  val projectVersion = "0.0.1"
  val akkaVersion = "2.3.9"
  val scalatestVersion = "2.2.4"
  val scalaLangVersion = "2.11.5"

  val commonScalacOptions = Seq("-feature", "-deprecation")
  val commonJavacOptions = Seq("-source", "1.7")

  lazy val akkaSmrBuild = (project in file("."))
    .settings(SbtMultiJvm.multiJvmSettings: _*)
    .settings(

      name := "akka-smr",
      version := projectVersion,

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := true,

      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "joda-time" % "joda-time" % "2.3",
        "org.joda" % "joda-convert" % "1.6",
        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.0.13" % "test"
      ),

      // make sure that MultiJvm test are compiled by the default test compilation
      compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),

      // disable parallel tests
      parallelExecution in Test := false,

      // make sure that MultiJvm tests are executed by the default test target
      executeTests in Test <<=
        (executeTests in Test, executeTests in MultiJvm) map {
          case ((testResults), (multiJvmResults)) =>
            val overall =
              if (testResults.overall.id < multiJvmResults.overall.id)
                multiJvmResults.overall
              else
                testResults.overall
            Tests.Output(overall,
              testResults.events ++ multiJvmResults.events,
              testResults.summaries ++ multiJvmResults.summaries)
        }
    )
}
