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
package akkeeper.container

import java.io.File

import akkeeper.utils.CliArguments._
import scopt.OptionParser

import scala.util.control.NonFatal

object ContainerInstanceMain extends App {

  val optParser = new OptionParser[ContainerInstanceArguments]("akkeeperInstance") {
    head("akkeeperInstance", "0.1")

    opt[String](AppIdArg).required().action((v, c) => {
      c.copy(appId = v)
    }).text("ID of this application")

    opt[String](InstanceIdArg).required().action((v, c) => {
      c.copy(instanceId = v)
    }).text("ID of this instance")

    opt[String](MasterAddressArg).required().action((v, c) => {
      c.copy(masterAddress = v)
    }).text("master instance address")

    opt[File](ConfigArg).valueName("<file>").required().action((v, c) => {
      c.copy(config = v)
    }).text("custom configuration file")
  }

  def run(instanceArgs: ContainerInstanceArguments): Unit = {
    // TODO:
  }

  try {
    optParser.parse(args, ContainerInstanceArguments()).foreach(run)
  } catch {
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(1)
  }
}
