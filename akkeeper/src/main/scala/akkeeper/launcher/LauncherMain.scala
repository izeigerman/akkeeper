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
package akkeeper.launcher

import java.io.File

import akkeeper.launcher.yarn.YarnLauncher
import akkeeper.utils.yarn.YarnUtils
import com.typesafe.config.ConfigFactory
import scopt.OptionParser
import scala.util.control.NonFatal

object LauncherMain extends App {

  val optParser = new OptionParser[LaunchArguments]("akkeeper") {
    head("akkeeper", "0.1")

    opt[File]("akkeeperJar").required().action((v, c) => {
      c.copy(akkeeperJarPath = v)
    }).text("path to the Akkeeper Jar file")

    opt[Seq[File]]("jars").valueName("<jar1>,<jar2>...").action((v, c) => {
      c.copy(otherJars = v)
    }).text("additional Jars that should be included into the classpath")

    opt[Seq[File]]("resources").valueName("<file1>,<file2>..").action((v, c) => {
      c.copy(resources = v)
    }).text("any resource files that have to be distributed within a cluster")

    opt[Seq[String]]("masterJvmArgs").valueName("<prop1>,<prop2>...").action((v, c) => {
      c.copy(masterJvmArgs = v)
    }).text("extra JVM arguments for the Akeeper master")

    opt[File]("config").valueName("<file>").action((v, c) => {
      c.copy(config = Some(v))
    }).text("custom configuration file")

    arg[File]("<file>").required().action((x, c) => {
      c.copy(userJar = x)
    }).text("a user Jar")
  }

  def run(launcherArgs: LaunchArguments): Unit = {
    val config = launcherArgs.config
      .map(c => ConfigFactory.parseFile(c).withFallback(ConfigFactory.load()))
      .getOrElse(ConfigFactory.load())
    val launcher = new YarnLauncher(YarnUtils.getYarnConfiguration)
    launcher.start()
    launcher.launch(config, launcherArgs)
    launcher.stop()
  }

  try {
    optParser.parse(args, LaunchArguments()).foreach(run)
  } catch {
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(1)
  }
}
