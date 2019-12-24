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

import java.net.URI

import akkeeper.common.config.AkkeeperResource
import akkeeper.launcher.LaunchArguments._
import com.typesafe.config.Config

final case class LaunchArguments(akkeeperJarPath: URI = new URI("."),
                                 userJar: URI = new URI("."),
                                 otherJars: Seq[URI] = Seq.empty,
                                 resources: Seq[URI] = Seq.empty,
                                 globalResources: Seq[AkkeeperResource] = Seq.empty,
                                 masterJvmArgs: Seq[String] = Seq.empty,
                                 userConfig: Option[Config] = None,
                                 pollInterval: Long = DefaultPollInterval,
                                 yarnQueue: Option[String] = None,
                                 principal: Option[String] = None,
                                 keytab: Option[URI] = None)

object LaunchArguments {
  val DefaultPollInterval = 1000
}
