/*
 * Copyright 2017 Iaroslav Zeigerman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt._
import Keys._
import sbtassembly._
import AssemblyKeys._

object AkkeeperBuild extends Build {

  val AkkaVersion = "2.3.16"
  val CuratorVersion = "2.4.0"
  val SprayJsonVersion = "1.3.3"
  val HadoopVersion = "2.6.2"
  val ScalaTestVersion = "2.2.6"
  val ScalamockVersion = "3.4.2"
  val Slf4jVersion = "1.7.19"
  val ScoptsVersion = "3.5.0"

  val HadoopDependencies = Seq(
    "org.apache.hadoop" % "hadoop-common" % HadoopVersion,
    "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion,
    "org.apache.hadoop" % "hadoop-yarn-common" % HadoopVersion,
    "org.apache.hadoop" % "hadoop-yarn-client" % HadoopVersion,
    ("org.apache.curator" % "curator-framework" % CuratorVersion).exclude("org.jboss.netty", "netty"),
    "org.apache.curator" % "curator-test" % CuratorVersion % "test->*"
  ).map(_.exclude("log4j", "log4j"))

  val CommonSettings = Seq(
    organization := "akkeeper",
    scalaVersion := "2.11.7",
    version := "0.1-SNAPSHOT",

    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature",
      "-language:postfixOps",
      "-language:implicitConversions",
      "-language:higherKinds"),

    parallelExecution in Test := false,

    libraryDependencies ++= HadoopDependencies ++ Seq(
      "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % AkkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
      "io.spray" %% "spray-json" % SprayJsonVersion,
      "org.slf4j" % "slf4j-api" % Slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % Slf4jVersion,
      "com.github.scopt" %% "scopt" % ScoptsVersion,
      "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % "test->*",
      "org.scalatest" %% "scalatest" % ScalaTestVersion % "test->*",
      "org.scalamock" %% "scalamock-scalatest-support" % ScalamockVersion % "test->*"
    )
  )

  val AkkeeperSettings = CommonSettings ++ Seq(
    mainClass in Compile := Some("akkeeper.launcher.LauncherMain"),
    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.first
      case "log4j.properties" => MergeStrategy.concat
      case "reference.conf" => ReferenceMergeStrategy
      case "application.conf" => MergeStrategy.concat
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  val NoPublishSettings = CommonSettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  lazy val root = Project(id = "root", base = file("."))
    .settings(NoPublishSettings: _*)
    .aggregate(akkeeper)
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val akkeeper = Project(id = "akkeeper", base = file("akkeeper"))
    .settings(AkkeeperSettings: _*)
}
