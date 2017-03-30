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

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.Cluster
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import akkeeper.deploy.{DeployClient, DeployClientFactory}
import akkeeper.deploy.yarn.YarnApplicationMasterConfig
import akkeeper.master.route._
import akkeeper.master.service.MasterService
import akkeeper.utils.ConfigUtils._
import akkeeper.storage.{InstanceStorage, InstanceStorageFactory}
import akkeeper.utils.yarn._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

private[master] trait MasterRunner {
  def run(masterArgs: MasterArguments): Unit
}

private[master] class YarnMasterRunner extends MasterRunner {

  private val logger = LoggerFactory.getLogger(classOf[YarnMasterRunner])

  private def createInstanceStorage(actorSystem: ActorSystem,
                                    appId: String): InstanceStorage.Async = {
    val zkConfig = actorSystem.settings.config.getZookeeperClientConfig
    InstanceStorageFactory.createAsync(zkConfig.child(appId))
  }

  private def createDeployClient(actorSystem: ActorSystem,
                                 masterArgs: MasterArguments,
                                 restApiPort: Int): DeployClient.Async = {
    val yarnConf = YarnUtils.getYarnConfiguration
    val config = actorSystem.settings.config
    val selfAddr = Cluster(actorSystem).selfAddress
    val principal = masterArgs.principal

    val trackingUrl = s"http://${selfAddr.host.get}:${restApiPort}/api/v1"
    val yarnConfig = YarnApplicationMasterConfig(
      config = config, yarnConf = yarnConf,
      appId = masterArgs.appId, selfAddress = selfAddr, trackingUrl = trackingUrl,
      principal = principal)
    DeployClientFactory.createAsync(yarnConfig)
  }

  private def loginAndGetRenewer(config: Config, principal: String): KerberosTicketRenewer = {
    val loginUser = YarnUtils.loginFromKeytab(principal, LocalResourceNames.KeytabName)
    new KerberosTicketRenewer(
      loginUser,
      config.getDuration("akkeeper.kerberos.ticket-check-interval", TimeUnit.MILLISECONDS))
  }

  private def createRestHandler(restConfig: Config, masterService: ActorRef)
                               (implicit dispatcher: ExecutionContext): Route = {
    implicit val timeout = Timeout(
      restConfig.getDuration("request-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

    ControllerComposite("api/v1", Seq(
      DeployController(masterService),
      ContainerController(masterService)
    )).route
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

    val restConfig = config.getRestConfig
    val restPort = restConfig.getInt("port")

    val masterConfig = config.withMasterPort.withMasterRole
    implicit val actorSystem = ActorSystem(config.getActorSystemName, masterConfig)
    implicit val materializer = ActorMaterializer()
    implicit val dispatcher = actorSystem.dispatcher

    val instanceStorage = createInstanceStorage(actorSystem, masterArgs.appId)
    val deployClient = createDeployClient(actorSystem, masterArgs, restPort)
    val masterService = MasterService.createLocal(actorSystem, deployClient, instanceStorage)

    val restHandler = createRestHandler(restConfig, masterService)
    Http().bindAndHandle(restHandler, "0.0.0.0", restPort).onFailure {
      case ex: Exception =>
        logger.error(s"Failed to bind to port $restPort", ex)
    }

    actorSystem.awaitTermination()
    ticketRenewer.foreach(_.stop())
  }
}
