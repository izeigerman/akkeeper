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
package akkeeper.master

import akka.actor._
import akka.cluster.Cluster
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import akkeeper.common.config._
import akkeeper.deploy.{DeployClient, DeployClientFactory}
import akkeeper.deploy.yarn.YarnApplicationMasterConfig
import akkeeper.master.route._
import akkeeper.master.service.MasterService
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import akkeeper.storage.{InstanceStorage, InstanceStorageFactory}
import akkeeper.yarn._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

private[master] trait MasterRunner {
  def run(masterArgs: MasterArguments): Unit
}

private[master] object MasterRunner {
  final class ServiceProxy extends Actor with Stash {
    private var service: Option[ActorRef] = None

    override def receive: Receive = waitingForServiceReceive

    private val waitingForServiceReceive: Receive = {
      case newService: ActorRef =>
        service = Some(newService)
        context.become(proxyReceive)
        unstashAll()
      case _ => stash()
    }

    private val proxyReceive: Receive = {
      case message => service.get.forward(message)
    }
  }
}

private[master] class YarnMasterRunner extends MasterRunner {

  private val logger = LoggerFactory.getLogger(classOf[YarnMasterRunner])

  private def createInstanceStorage(actorSystem: ActorSystem,
                                    appId: String): InstanceStorage = {
    val zkConfig = ZookeeperClientConfig.fromConfig(actorSystem.settings.config.zookeeper)
    InstanceStorageFactory(zkConfig.child(appId))
  }

  private def createDeployClient(actorSystem: ActorSystem,
                                 masterArgs: MasterArguments,
                                 restApiPort: Int): DeployClient = {
    val yarnConf = YarnUtils.getYarnConfiguration
    val config = actorSystem.settings.config
    val selfAddr = Cluster(actorSystem).selfAddress
    val principal = masterArgs.principal

    val trackingUrl = s"http://${selfAddr.host.get}:${restApiPort}/api/v1"
    val yarnConfig = YarnApplicationMasterConfig(
      config = config, yarnConf = yarnConf,
      appId = masterArgs.appId, selfAddress = selfAddr, trackingUrl = trackingUrl,
      principal = principal)
    DeployClientFactory(yarnConfig)
  }

  private def loginAndGetRenewer(config: Config, principal: String): KerberosTicketRenewer = {
    val loginUser = YarnUtils.loginFromKeytab(principal, LocalResourceNames.KeytabName)
    new KerberosTicketRenewer(loginUser, config.kerberos.ticketCheckInterval)
  }

  private def createRestHandler(config: Config, masterService: ActorRef)
                               (implicit dispatcher: ExecutionContext): Route = {
    implicit val timeout = Timeout(config.rest.requestTimeout)

    ControllerComposite("api/v1", Seq(
      DeployController(masterService),
      ContainerController(masterService),
      MonitoringController(masterService),
      MasterController(masterService)
    )).route
  }

  private def bindHttp(handler: Route, port: Int, attemptsLeft: Int)
                      (implicit system: ActorSystem, context: ExecutionContext,
                       materializer: ActorMaterializer): Future[Int] = {
    Http().bindAndHandle(handler, "0.0.0.0", port)
      .map { binding =>
        binding.localAddress.getPort
      }
      .recoverWith { case ex =>
        logger.error(s"Failed to bind to port $port", ex)
        if (attemptsLeft == 0) {
          Future.failed(ex)
        } else {
          bindHttp(handler, port + 1, attemptsLeft - 1)
        }
      }
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

    val restPort = config.rest.port
    val restPortMaxAttempts = config.rest.portMaxAttempts

    val masterConfig = config.withMasterPort.withMasterRole
    implicit val actorSystem = ActorSystem(config.akkeeperAkka.actorSystemName, masterConfig)
    implicit val materializer = ActorMaterializer()
    implicit val dispatcher = actorSystem.dispatcher

    val masterServiceProxy = actorSystem.actorOf(Props(classOf[MasterRunner.ServiceProxy]))
    val restHandler = createRestHandler(config, masterServiceProxy)
    val actualRestPort = Await.result(bindHttp(restHandler, restPort, restPortMaxAttempts), Duration.Inf)

    val instanceStorage = createInstanceStorage(actorSystem, masterArgs.appId)
    val deployClient = createDeployClient(actorSystem, masterArgs, actualRestPort)
    val masterService = MasterService.createLocal(actorSystem, deployClient, instanceStorage)
    masterServiceProxy ! masterService

    Await.result(actorSystem.whenTerminated, Duration.Inf)
    materializer.shutdown()
    ticketRenewer.foreach(_.stop())
  }
}
