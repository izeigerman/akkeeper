/*
 * Copyright 2017-2018 Iaroslav Zeigerman
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

val AkkaVersion = "2.5.14"
val AkkaHttpVersion = "10.1.3"
val CuratorVersion = "2.7.1"
val SprayJsonVersion = "1.3.4"
val HadoopVersion = "2.8.4"
val ScalaTestVersion = "3.0.5"
val ScalamockVersion = "3.4.2"
val Slf4jVersion = "1.7.19"
val ScoptsVersion = "3.5.0"

val HadoopDependencies = Seq(
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-yarn-common" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-yarn-client" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % "provided",
  ("org.apache.curator" % "curator-framework" % CuratorVersion).exclude("org.jboss.netty", "netty"),
  "org.apache.curator" % "curator-test" % CuratorVersion % "test->*"
).map(_.exclude("log4j", "log4j"))

val CommonSettings = Seq(
  organization := "com.github.izeigerman",
  scalaVersion := "2.12.6",
  crossScalaVersions := Seq("2.11.11", scalaVersion.value),
  version := "0.2.3-SNAPSHOT",

  organizationHomepage := Some(url("https://github.com/izeigerman")),
  homepage := Some(url("https://github.com/izeigerman/akkeeper")),
  licenses in ThisBuild += ("MIT License", url("http://opensource.org/licenses/MIT")),
  developers := Developer("izeigerman", "Iaroslav Zeigerman", "",
    url("https://github.com/izeigerman")) :: Nil,
  scmInfo := Some(ScmInfo(
    browseUrl = url("https://github.com/izeigerman/akkeeper.git"),
    connection = "scm:git:git@github.com:izeigerman/akkeeper.git"
  )),

  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-language:postfixOps",
    "-language:implicitConversions",
    "-language:higherKinds"),

  parallelExecution in Test := false
)

val AkkeeperSettings = CommonSettings ++ Seq(
  libraryDependencies ++= HadoopDependencies ++ Seq(
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-cluster" % AkkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
    "io.spray" %% "spray-json" % SprayJsonVersion,
    "org.slf4j" % "slf4j-api" % Slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % Slf4jVersion,
    "com.github.scopt" %% "scopt" % ScoptsVersion,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % "test->*",
    "org.scalatest" %% "scalatest" % ScalaTestVersion % "test->*",
    "org.scalamock" %% "scalamock-scalatest-support" % ScalamockVersion % "test->*"
  ),

  test in assembly := {},

  mainClass in Compile := Some("akkeeper.launcher.LauncherMain"),

  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case "log4j.properties" => MergeStrategy.concat
    case "reference.conf" => ReferenceMergeStrategy
    case "application.conf" => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },

  packageName in Universal := {
    val scalaVer = scalaVersion.value
    name.value + "_" + scalaVer.substring(0, scalaVer.lastIndexOf('.')) + "-" + version.value
  },

  topLevelDirectory in Universal := Some(name.value + "-" + version.value),

  mappings in Universal := {
    val fatJar = (assembly in Compile).value
    Seq(
      new File(baseDirectory.value.getAbsolutePath, "../README.md") -> "README.md",
      new File(baseDirectory.value.getAbsolutePath, "../LICENSE") -> "LICENSE",
      new File(baseDirectory.value.getAbsolutePath, "../bin/akkeeper-submit") -> "bin/akkeeper-submit",
      fatJar -> ("lib/" + fatJar.getName)
    )
  },

  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := (_ => false),
  publishTo := Some(
    if (version.value.trim.endsWith("SNAPSHOT")) {
      "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    } else {
      "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"
    }
  ),

  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion),
  buildInfoPackage := "akkeeper"
)

val NoPublishSettings = CommonSettings ++ Seq(
  publishArtifact := false,
  publish := {},
  skip in publish := true,
  coverageEnabled := false
)

lazy val root = Project(id = "root", base = file("."))
  .settings(NoPublishSettings: _*)
  .aggregate(akkeeper, akkeeperExamples)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)

lazy val akkeeper = Project(id = "akkeeper", base = file("akkeeper"))
  .settings(AkkeeperSettings: _*)
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(BuildInfoPlugin)

lazy val akkeeperExamples = Project(id = "akkeeper-examples", base = file("akkeeper-examples"))
  .settings(NoPublishSettings: _*)
  .dependsOn(akkeeper)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)
