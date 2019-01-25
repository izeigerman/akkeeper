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
val TypesafeConfigVersion = "1.3.3"

val HadoopDependencies = Seq(
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-yarn-common" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-yarn-client" % HadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % "provided"
)

val TestDependencies = Seq(
  "org.scalatest" %% "scalatest" % ScalaTestVersion % "test->*",
  "org.scalamock" %% "scalamock-scalatest-support" % ScalamockVersion % "test->*"
)

val CommonSettings = Seq(
  organization := "com.github.izeigerman",
  scalaVersion := "2.12.6",
  crossScalaVersions := Seq("2.11.11", scalaVersion.value),
  version := "0.4.3-SNAPSHOT",

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

val PublishSettings = Seq(
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := (_ => false),
  publishTo := Some(
    if (version.value.trim.endsWith("SNAPSHOT")) {
      "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    } else {
      "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"
    }
  )
)

val NoPublishSettings = Seq(
  publishArtifact := false,
  publish := {},
  skip in publish := true
)

val BuildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion),
  buildInfoPackage := "akkeeper"
)

val AkkeeperAppSettings = CommonSettings ++ NoPublishSettings ++ BuildInfoSettings ++ Seq(
  libraryDependencies ++= HadoopDependencies.map(_.exclude("log4j", "log4j")) ++ TestDependencies ++ Seq(
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
    ("org.apache.curator" % "curator-framework" % CuratorVersion).exclude("org.jboss.netty", "netty"),
    "org.apache.curator" % "curator-test" % CuratorVersion % "test->*",
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % "test->*"
  ),

  test in assembly := {},

  mainClass in Compile := Some("akkeeper.launcher.LauncherMain"),

  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case "log4j.properties" => MergeStrategy.concat
    case "reference.conf" => ReferenceMergeStrategy
    case "application.conf" => MergeStrategy.concat
    case "akkeeper/BuildInfo$.class" => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },

  assemblyJarName in assembly := { s"akkeeper-assembly-${version.value}.jar" },

  packageName in Universal := {
    val scalaVer = scalaVersion.value
    "akkeeper_" + scalaVer.substring(0, scalaVer.lastIndexOf('.')) + "-" + version.value
  },

  topLevelDirectory in Universal := Some("akkeeper-" + version.value),

  mappings in Universal := {
    val fatJar = (assembly in Compile).value
    Seq(
      new File(baseDirectory.value.getAbsolutePath, "../README.md") -> "README.md",
      new File(baseDirectory.value.getAbsolutePath, "../LICENSE") -> "LICENSE",
      new File(baseDirectory.value.getAbsolutePath, "../bin/akkeeper-submit") -> "bin/akkeeper-submit",
      fatJar -> ("lib/" + fatJar.getName)
    )
  }
)

val AkkeeperApiSettings = CommonSettings ++ PublishSettings ++ Seq(
  libraryDependencies ++= TestDependencies ++ Seq(
    "io.spray" %% "spray-json" % SprayJsonVersion
  )
)

val AkkeeperCommonSettings = CommonSettings ++ PublishSettings ++ Seq(
  libraryDependencies ++= Seq(
    "io.spray" %% "spray-json" % SprayJsonVersion,
    "com.typesafe" % "config" % TypesafeConfigVersion
  )
)

val AkkeeperYarnSettings = CommonSettings ++ PublishSettings ++ Seq(
  libraryDependencies ++= HadoopDependencies ++ TestDependencies
)

val AkkeeperLauncherSettings = CommonSettings ++ PublishSettings ++ BuildInfoSettings ++ Seq(
  libraryDependencies ++= HadoopDependencies ++ TestDependencies ++ Seq(
    "com.github.scopt" %% "scopt" % ScoptsVersion
  )
)

lazy val akkeeperRoot = Project(id = "akkeeper", base = file("."))
  .settings(CommonSettings: _*)
  .settings(NoPublishSettings: _*)
  .settings(coverageEnabled := false)
  .aggregate(akkeeperApi, akkeeperCommon, akkeeperYarn, akkeeperLauncher, akkeeperApp, akkeeperExamples)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)

lazy val akkeeperApi = Project(id = "akkeeper-api", base = file("akkeeper-api"))
  .settings(AkkeeperApiSettings: _*)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)

lazy val akkeeperCommon = Project(id = "akkeeper-common", base = file("akkeeper-common"))
  .settings(AkkeeperCommonSettings: _*)
  .dependsOn(akkeeperApi)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)

lazy val akkeeperYarn = Project(id = "akkeeper-yarn", base = file("akkeeper-yarn"))
  .settings(AkkeeperYarnSettings: _*)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)

lazy val akkeeperLauncher = Project(id = "akkeeper-launcher", base = file("akkeeper-launcher"))
  .settings(AkkeeperLauncherSettings: _*)
  .dependsOn(akkeeperApi)
  .dependsOn(akkeeperCommon)
  .dependsOn(akkeeperYarn)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)
  .enablePlugins(BuildInfoPlugin)

lazy val akkeeperApp = Project(id = "akkeeper-app", base = file("akkeeper"))
  .settings(AkkeeperAppSettings: _*)
  .dependsOn(akkeeperApi)
  .dependsOn(akkeeperCommon)
  .dependsOn(akkeeperYarn)
  .dependsOn(akkeeperLauncher)
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(BuildInfoPlugin)

lazy val akkeeperExamples = Project(id = "akkeeper-examples", base = file("akkeeper-examples"))
  .settings(CommonSettings: _*)
  .settings(NoPublishSettings: _*)
  .settings(coverageEnabled := false)
  .dependsOn(akkeeperApp)
  .disablePlugins(sbtassembly.AssemblyPlugin, JavaAppPackaging)
