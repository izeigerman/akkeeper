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
package akkeeper.master

import java.io.File

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akkeeper.deploy.{DeployClientFactory, DeployClient}
import akkeeper.deploy.yarn.YarnApplicationMasterConfig
import akkeeper.master.service.MasterService
import akkeeper.storage.{InstanceStorageFactory, InstanceStorage}
import akkeeper.utils.ConfigUtils._
import akkeeper.utils.CliArguments._
import akkeeper.utils.yarn.YarnUtils
import com.typesafe.config._
import scopt.OptionParser
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

object MasterMain extends App {

  val optParser = new OptionParser[MasterArguments]("akkeeperMaster") {
    head("akkeeperMaster", "0.1")

    opt[String](AppIdArg).required().action((v, c) => {
      c.copy(appId = v)
    }).text("ID of this application")

    opt[File](ConfigArg).valueName("<file>").optional().action((v, c) => {
      c.copy(config = Some(v))
    }).text("custom configuration file")
  }


  def createInstanceStorage(actorSystem: ActorSystem, appId: String): InstanceStorage.Async = {
    val zkConfig = actorSystem.settings.config.getZookeeperClientConfig
    InstanceStorageFactory.createAsync(zkConfig.child(appId))
  }

  def createDeployClient(actorSystem: ActorSystem, appId: String): DeployClient.Async = {
    val yarnConf = YarnUtils.getYarnConfiguration
    val config = actorSystem.settings.config
    val selfAddr = Cluster(actorSystem).selfAddress
    val yarnConfig = YarnApplicationMasterConfig(config, yarnConf, appId, selfAddr, "")
    DeployClientFactory.createAsync(yarnConfig)
  }

  def run(masterArgs: MasterArguments): Unit = {
    val config = masterArgs.config
      .map(c => ConfigFactory.parseFile(c).withFallback(ConfigFactory.load()))
      .getOrElse(ConfigFactory.load())

    val masterConfig = config.withMasterPort.withMasterRole
    val actorSystem = ActorSystem(config.getActorSystemName, masterConfig)

    val instanceStorage = createInstanceStorage(actorSystem, masterArgs.appId)
    val deployClient = createDeployClient(actorSystem, masterArgs.appId)

    MasterService.createLocal(actorSystem, deployClient, instanceStorage)
    actorSystem.awaitTermination()
    sys.exit()
  }

  try {
    optParser.parse(args, MasterArguments()).foreach(run)
  } catch {
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(1)
  }
}
