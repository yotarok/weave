import sbt._
import sbt.Keys._

// for sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._
import org.scalastyle.sbt.ScalastylePlugin._

object WeaveBuild extends Build {

  val _scalaVersion = "2.11.12"

  val _commonLibraryDependencies = Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.3.3",
    "com.typesafe.akka" %% "akka-kernel" % "2.3.3",
    "com.typesafe.akka" %% "akka-remote" % "2.3.3",
    "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
    "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
    "org.rogach" %% "scallop" % "3.1.1",
    "com.typesafe.play" %% "play-json" % "2.3.0",
    "org.scala-lang.modules" %% "scala-pickling" % "0.10.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "org.slf4j" % "slf4j-simple" % "1.7.24"
  )

  val paradiseVersion = "2.1.0"

  val buildSettings = Seq(
    organization := "ro.yota",
    version := "0.1-SNAPSHOT",
    scalacOptions ++= Seq("-feature", "-deprecation"),
    scalaVersion := _scalaVersion,
    resolvers += Resolver.sonatypeRepo("public"),
    resolvers += Resolver.sonatypeRepo("releases"),
    resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "JCenter" at "http://jcenter.bintray.com/",
    resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  )

  lazy val root = (project in file("."))
    .settings(buildSettings: _*)
    .settings(
      run <<= run in Compile in weaveCore)
    .aggregate(weaveMacro, weaveCore, weaveStdLib, weaveSchd)

  lazy val weaveMacro = (project in file("macrodef"))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
      libraryDependencies ++= (
        if (scalaVersion.value.startsWith("2.10")) List("org.scalamacros" %% "quasiquotes" % paradiseVersion)
        else Nil
      )
    )

  lazy val weaveCore = (project in file("core"))
    .settings(buildSettings: _*)
    .settings(
      mainClass in (Compile, packageBin) := Some("ro.yota.weave.Weave"),
      // set the main class afor the main 'run' task
      // change Compile to Test to set it for 'test:run'
      mainClass in (Compile, run) := Some("ro.yota.weave.Weave"),
      test in assembly := {},
      fork in Test := true, // required for running test exercising ScriptEngine
       // add other settings here
      libraryDependencies ++= _commonLibraryDependencies,
      libraryDependencies ++= Seq(
        "jline" % "jline" % "2.14.3",
        "io.atlassian.aws-scala" %% "aws-scala-s3"  % "4.0.2",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test",
        "com.typesafe.slick" %% "slick" % "3.2.1",
        "org.xerial" % "sqlite-jdbc" % "3.8.6",
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
        "io.findify" %% "s3mock" % "0.2.4" % "test"
      )
    )
    .dependsOn(weaveMacro, weaveSchd)

  lazy val weaveSchd = (project in file("schd"))
    .settings(buildSettings: _*)
    .settings(
      mainClass in (Compile, packageBin) := Some("ro.yota.weaveschd.SchdMain"),
      // set the main class afor the main 'run' task
      // change Compile to Test to set it for 'test:run'
      mainClass in (Compile, run) := Some("ro.yota.weaveschd.SchdMain"),
      // add other settings here
      libraryDependencies ++= _commonLibraryDependencies
    )
    .dependsOn(weaveMacro)

  lazy val weaveStdLib = (project in file("stdlib"))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= (
        List(
          "net.databinder.dispatch" %% "dispatch-core" % "0.11.1",
          "org.yaml" % "snakeyaml" % "1.9",
          "com.nrinaudo" %% "kantan.csv" % "0.2.0"
        )
      )
    )
    .dependsOn(weaveMacro % "provided", weaveCore % "provided")

}
