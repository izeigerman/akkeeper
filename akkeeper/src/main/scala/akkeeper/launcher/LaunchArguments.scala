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

import com.typesafe.config.Config
import LaunchArguments._

private[akkeeper] case class LaunchArguments(akkeeperJarPath: File = new File("."),
                                             userJar: File = new File("."),
                                             otherJars: Seq[File] = Seq.empty,
                                             resources: Seq[File] = Seq.empty,
                                             masterJvmArgs: Seq[String] = Seq.empty,
                                             userConfig: Option[Config] = None,
                                             pollInterval: Long = DefaultPollInterval,
                                             principal: Option[String],
                                             keytab: File = new File("."))

object LaunchArguments {
  val DefaultPollInterval = 1000
}
