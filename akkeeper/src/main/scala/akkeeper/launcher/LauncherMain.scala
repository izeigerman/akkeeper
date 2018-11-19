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
package akkeeper.launcher

import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

import akkeeper.BuildInfo
import akkeeper.utils.yarn.YarnUtils
import com.typesafe.config.ConfigFactory
import scopt.OptionParser

import scala.concurrent.Await
import scala.util.control.NonFatal
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object LauncherMain extends App {

  private[akkeeper] def transformUri(uri: URI): URI = {
    val uriStr = uri.toString
    if (Option(uri.getScheme).isEmpty) {
      // If the scheme is not specified, use the "file" scheme as a default choice.
      if (uriStr.startsWith("/")) {
        // Absolute path.
        new URI("file://" + uriStr)
      } else {
        // Relative path.
        new URI("file://" + new File(uriStr).getAbsolutePath)
      }
    } else {
      uri
    }
  }

  private val optParser = new OptionParser[LaunchArguments](BuildInfo.name) {
    head(BuildInfo.name, BuildInfo.version)

    opt[URI]("akkeeperJar").required().action((v, c) => {
      c.copy(akkeeperJarPath = transformUri(v))
    }).text("Path to the Akkeeper fat Jar.")

    opt[Seq[URI]]("jars").valueName("<jar1>,<jar2>,...").action((v, c) => {
      c.copy(otherJars = v.map(transformUri))
    }).text("A comma-separated list of additional Jar files that have to be included into " +
      "the container's classpath.")

    opt[Seq[URI]]("resources").valueName("<file1>,<file2>,...").action((v, c) => {
      c.copy(resources = v.map(transformUri))
    }).text("A comma-separated list of resource files that have to be distributed within a cluster.")

    opt[Seq[String]]("masterJvmArgs").valueName("<prop1>,<prop2>,...").action((v, c) => {
      c.copy(masterJvmArgs = v)
    }).text("Extra JVM arguments for the Akeeper master.")

    opt[String]("queue").valueName("<YARN queue>").action((v, c) => {
      c.copy(yarnQueue = Some(v))
    }).text("The YARN queue (default: 'default')")

    opt[String]("principal").valueName("principal").action((v, c) => {
      c.copy(principal = Some(v))
    }).text("Principal to be used to login to KDC.")

    opt[URI]("keytab").valueName("<keytab path>").action((v, c) => {
      c.copy(keytab = transformUri(v))
    }).text("The full path to the file that contains the keytab for the principal specified above.")

    opt[File]("config").valueName("<file>").action((v, c) => {
      c.copy(userConfig = Some(ConfigFactory.parseFile(v)))
    }).text("The path to the custom configuration file.")

    arg[URI]("<jar>").required().action((x, c) => {
      c.copy(userJar = transformUri(x))
    }).text("The path to the user Jar file.")
  }

  private val DefaultLauncherTimeout = 90 seconds

  private def runYarn(launcherArgs: LaunchArguments): Unit = {
    val config = launcherArgs.userConfig
      .map(c => c.withFallback(ConfigFactory.load()))
      .getOrElse(ConfigFactory.load())

    launcherArgs.principal.foreach(p => {
      YarnUtils.loginFromKeytab(p, launcherArgs.keytab.toString)
    })

    val launcherTimeout =
      if (config.hasPath("akkeeper.launcher.timeout")) {
        config.getDuration("akkeeper.launcher.timeout", TimeUnit.SECONDS).seconds
      } else {
        DefaultLauncherTimeout
      }
    val launcher = Launcher.createYarnLauncher(YarnUtils.getYarnConfiguration)
    val launchResult = launcher.launch(config, launcherArgs)
    Await.result(launchResult, launcherTimeout)
  }

  def run(launcherArgs: LaunchArguments): Unit = {
    runYarn(launcherArgs)
  }

  try {
    optParser.parse(args, LaunchArguments()).foreach(run)
  } catch {
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(1)
  }
}
