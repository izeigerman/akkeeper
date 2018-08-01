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

import akkeeper.launcher.yarn.YarnLauncher
import akkeeper.utils.yarn.YarnUtils
import com.typesafe.config.{Config, ConfigFactory}
import scopt.OptionParser
import scala.concurrent.Await
import scala.util.control.NonFatal
import scala.concurrent.duration._

object LauncherMain extends App {

  val optParser = new OptionParser[LaunchArguments]("akkeeper") {
    head("akkeeper", "0.1")

    opt[File]("akkeeperJar").required().action((v, c) => {
      c.copy(akkeeperJarPath = v)
    }).text("path to the Akkeeper fat Jar file")

    opt[Seq[File]]("jars").valueName("<jar1>,<jar2>...").action((v, c) => {
      c.copy(otherJars = v)
    }).text("additional Jars that should be included into the classpath")

    opt[Seq[File]]("resources").valueName("<file1>,<file2>..").action((v, c) => {
      c.copy(resources = v)
    }).text("any resource files that have to be distributed within a cluster")

    opt[Seq[String]]("masterJvmArgs").valueName("<prop1>,<prop2>...").action((v, c) => {
      c.copy(masterJvmArgs = v)
    }).text("extra JVM arguments for the Akeeper master")

    opt[String]("principal").valueName("principal").action((v, c) => {
      c.copy(principal = Some(v))
    })

    opt[File]("keytab").valueName("<keytab_file>").action((v, c) => {
      c.copy(keytab = v)
    })

    opt[File]("config").valueName("<file>").action((v, c) => {
      c.copy(userConfig = Some(ConfigFactory.parseFile(v)))
    }).text("custom configuration file")

    arg[File]("<jar>").required().action((x, c) => {
      c.copy(userJar = x)
    }).text("a user Jar")
  }

  val LauncherTimeout = 30 seconds

  private def runYarn(launcherArgs: LaunchArguments): Unit = {
    val config = launcherArgs.userConfig
      .map(c => c.withFallback(ConfigFactory.load()))
      .getOrElse(ConfigFactory.load())

    launcherArgs.principal.foreach(p => {
      YarnUtils.loginFromKeytab(p, launcherArgs.keytab.getAbsolutePath)
    })

    val launcher = new YarnLauncher(YarnUtils.getYarnConfiguration)
    launcher.start()
    val launchResult = launcher.launch(config, launcherArgs)
    Await.result(launchResult, LauncherTimeout)
    launcher.stop()
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
