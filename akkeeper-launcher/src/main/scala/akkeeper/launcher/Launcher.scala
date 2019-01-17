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

import akkeeper.launcher.yarn.YarnLauncher
import akkeeper.yarn.YarnLauncherClient
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.concurrent.{ExecutionContext, Future}

final case class LaunchResult(appId: String, masterHost: String)

/** Launcher for the Akkeeper application. */
trait Launcher {

  /** Launches the Akkeeper application and returns a launch result.
    *
    * @param config the app configuration.
    * @param args the launch arguments.
    * @return the wrapped result that contains an ID of the
    *         submitted application and the address of the node where
    *         Akkeeper Master is running.
    */
  def launch(config: Config, args: LaunchArguments): Future[LaunchResult]
}

object Launcher {
  def createYarnLauncher(yarnConfig: YarnConfiguration)
                        (implicit context: ExecutionContext): Launcher = {
    new YarnLauncher(yarnConfig, () => new YarnLauncherClient)
  }

  def createYarnLauncher(yarnConfig: Configuration)
                        (implicit context: ExecutionContext): Launcher = {
    createYarnLauncher(new YarnConfiguration(yarnConfig))
  }
}
