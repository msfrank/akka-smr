import sbt._
import Keys._

object AkkaSmrBuild extends Build {

  val projectVersion = "0.1"
  val akkaVersion = "2.2.3"

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
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "org.scalatest" %% "scalatest" % "2.0" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      )
    )
  )
}
