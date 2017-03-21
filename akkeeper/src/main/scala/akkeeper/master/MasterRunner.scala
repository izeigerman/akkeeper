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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akkeeper.deploy.{DeployClient, DeployClientFactory}
import akkeeper.deploy.yarn.YarnApplicationMasterConfig
import akkeeper.master.service.MasterService
import akkeeper.utils.ConfigUtils._
import akkeeper.storage.{InstanceStorage, InstanceStorageFactory}
import akkeeper.utils.yarn._
import com.typesafe.config.{Config, ConfigFactory}

private[master] trait MasterRunner {
  def run(masterArgs: MasterArguments): Unit
}

private[master] class YarnMasterRunner extends MasterRunner {
  private def createInstanceStorage(actorSystem: ActorSystem,
                                    appId: String): InstanceStorage.Async = {
    val zkConfig = actorSystem.settings.config.getZookeeperClientConfig
    InstanceStorageFactory.createAsync(zkConfig.child(appId))
  }

  private def createDeployClient(actorSystem: ActorSystem,
                                 masterArgs: MasterArguments): DeployClient.Async = {
    val yarnConf = YarnUtils.getYarnConfiguration
    val config = actorSystem.settings.config
    val selfAddr = Cluster(actorSystem).selfAddress
    val principal = masterArgs.principal

    val yarnConfig = YarnApplicationMasterConfig(
      config = config, yarnConf = yarnConf,
      appId = masterArgs.appId, selfAddress = selfAddr, trackingUrl = "",
      principal = principal)
    DeployClientFactory.createAsync(yarnConfig)
  }

  private def loginAndGetRenewer(config: Config, principal: String): KerberosTicketRenewer = {
    val loginUser = YarnUtils.loginFromKeytab(principal, LocalResourceNames.KeytabName)
    new KerberosTicketRenewer(
      loginUser,
      config.getDuration("akkeeper.kerberos.ticket-check-interval", TimeUnit.MILLISECONDS))
  }

  def run(masterArgs: MasterArguments): Unit = {
    val config = masterArgs.config
      .map(c => ConfigFactory.parseFile(c).withFallback(ConfigFactory.load()))
      .getOrElse(ConfigFactory.load())

    // Create and start the Kerberos ticket renewer if necessary.
    val ticketRenewer = masterArgs.principal.map(principal => {
      loginAndGetRenewer(config, principal)
    })
    ticketRenewer.foreach(_.start())

    val masterConfig = config.withMasterPort.withMasterRole
    val actorSystem = ActorSystem(config.getActorSystemName, masterConfig)

    val instanceStorage = createInstanceStorage(actorSystem, masterArgs.appId)
    val deployClient = createDeployClient(actorSystem, masterArgs)

    MasterService.createLocal(actorSystem, deployClient, instanceStorage)

    actorSystem.awaitTermination()
    ticketRenewer.foreach(_.stop())
  }
}
