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
package akkeeper.master.service

import akka.actor._
import akka.cluster.Cluster
import akkeeper.api._
import akkeeper.common.InstanceInfo
import akkeeper.deploy.DeployClient
import akkeeper.storage.InstanceStorage
import akkeeper.utils.ConfigUtils._
import scala.collection.mutable
import scala.collection.immutable
import MasterService._


private[akkeeper] class MasterService(deployClient: DeployClient.Async,
                                      instanceStorage: InstanceStorage.Async)
  extends Actor with ActorLogging with Stash {

  private val containerService: ActorRef = ContainerService.createLocal(context)
  private val monitoringService: ActorRef = MonitoringService.createLocal(context, instanceStorage)
  private val deployService: ActorRef = DeployService.createLocal(context, deployClient,
    containerService, monitoringService)

  private val cluster = Cluster(context.system)

  private val seedInstances: mutable.Set[InstanceInfo] = mutable.Set.empty
  private var instanceInfoRequired: Int = NumOfInstancesToJoin

  override def preStart(): Unit = {
    context.watch(containerService)
    context.watch(monitoringService)
    context.watch(deployService)
    monitoringService ! GetInstances()
    super.preStart()
  }

  private def stopServicesWithError(): Unit = {
    val error = MasterServiceException("Akkeeper Master Service fatal error")
    context.children.foreach(c => c ! StopWithError(error))
  }

  private def serviceTerminatedReceive: Receive = {
    case Terminated(actor) =>
      if (actor == containerService) {
        log.error("Container Service was terminated")
      } else if (actor == monitoringService) {
        log.error("Monitoring Service was terminated")
      } else {
        log.error("Deploy Service was terminated.")
      }
      if (context.children.isEmpty) {
        log.error("Can't proceed further. Shutting down the master")
        context.system.shutdown()
      }
  }

  private def apiReceive: Receive = {
    case r: DeployContainer => deployService.forward(r)
    case r: InstanceRequest => monitoringService.forward(r)
    case r: ContainerRequest => containerService.forward(r)
  }

  private def initialDeployReceive: Receive = {
    case DeployedInstances(_, container, instances) =>
      log.debug(s"Deployed ${instances.size} instances for container '$container'")
    case other: WithRequestId =>
      log.error(s"Initial deploy failed: $other")
  }

  private def initializedReceive: Receive = {
    apiReceive orElse serviceTerminatedReceive orElse initialDeployReceive
  }

  private def finishInit(): Unit = {
    log.info("Master service successfully initialized")

    context.become(initializedReceive)
    // Deploying instances specified in config.
    val deployRequests = context.system.settings.config.getDeployRequests
    log.debug(s"Deploying instances from config. " +
      s"Number of deploy requests: ${deployRequests.size}")
    deployRequests.foreach(r => deployService ! r)

    unstashAll()
  }

  private def joiningClusterReceive: Receive = {
    case InstancesList(_, instances) =>
      if (instances.isEmpty) {
        log.info("No running instances were found. Creating a new Akka cluster")
        cluster.join(cluster.selfAddress)
        finishInit()
      } else {
        log.info(s"Found ${instances.size} running instances. Joining the existing Akka cluster")
        // Choose N random instances to join.
        val seedNodeIds = scala.util.Random.shuffle(instances).take(NumOfInstancesToJoin)
        instanceInfoRequired = seedNodeIds.size
        seedNodeIds.foreach(id => monitoringService ! GetInstance(id))
      }

    case InstanceInfoResponse(_, info) =>
      seedInstances.add(info)
      log.debug(s"Received instance info. ${instanceInfoRequired - seedInstances.size} " +
        "more needed to proceed")
      if (seedInstances.size >= instanceInfoRequired) {
        val seedAddrs = immutable.Seq(seedInstances.map(_.address.get).toSeq: _*)
        cluster.joinSeedNodes(seedAddrs)
        finishInit()
      }

    case other @ (_: InstanceResponse | _: OperationFailed) =>
      log.error(s"Failed to retrieve information about instances. Initialization failed: $other")
      stopServicesWithError()

    case _: WithRequestId => stash()
  }

  private def uninitializedReceive: Receive = {
    joiningClusterReceive orElse serviceTerminatedReceive
  }

  override def receive: Receive = uninitializedReceive
}

object MasterService extends RemoteServiceFactory {
  val NumOfInstancesToJoin = 3

  val MasterServiceName = "akkeeperMaster"

  override val actorName = MasterServiceName

  private[akkeeper] def createLocal(factory: ActorRefFactory, deployClient: DeployClient.Async,
                                    instanceStorage: InstanceStorage.Async): ActorRef = {
    factory.actorOf(Props(classOf[MasterService], deployClient, instanceStorage), actorName)
  }
}
