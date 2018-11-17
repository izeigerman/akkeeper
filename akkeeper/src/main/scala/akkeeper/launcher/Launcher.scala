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

import akka.actor.Address
import com.typesafe.config.Config
import scala.concurrent.Future

case class LaunchResult(appId: String, masterAddress: Address)

/** Launcher for the Akkeeper application. */
trait Launcher {

  /** Launches the Akkeeper application and returns an asynchronous
    * launch result.
    *
    * @param config the app configuration.
    * @param args the launch arguments.
    * @return the asynchronous result that contains an ID of the
    *         submitted application and the address of the node where
    *         Akkeeper Master is running.
    */
  def launch(config: Config, args: LaunchArguments): Future[LaunchResult]
}
