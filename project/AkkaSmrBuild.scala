import sbt._
import Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

object AkkaSmrBuild extends Build {

  val projectVersion = "0.0.1"
  val akkaVersion = "2.3.0-RC4"

  lazy val akkaSmrBuild = Project(
    id = "akka-smr",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      exportJars := true,
      name := "akka-smr",
      version := projectVersion,
      scalaVersion := "2.10.3",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        //"com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "joda-time" % "joda-time" % "2.3",
        "org.joda" % "joda-convert" % "1.6",
        "org.scalatest" %% "scalatest" % "1.9.2" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.0.13" % "test"
      )
    ) ++ SbtMultiJvm.multiJvmSettings ++ Seq(
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
  ) configs MultiJvm
}
