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
package akkeeper.master.service

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akkeeper.api._
import akkeeper.common.InstanceInfo
import akkeeper.deploy.DeployClient
import akkeeper.master.service.MasterService._
import akkeeper.storage.InstanceStorage

import scala.collection.{immutable, mutable}


private[akkeeper] class MasterService(deployClient: DeployClient,
                                      instanceStorage: InstanceStorage.Async)
  extends Actor with ActorLogging with Stash {

  private val numOfInstancesToJoin: Int = context.system.settings.config
    .getInt("akkeeper.akka.seed-nodes-num")

  private val containerService: ActorRef = ContainerService.createLocal(context)
  private val monitoringService: ActorRef = MonitoringService.createLocal(context, instanceStorage)
  private val deployService: ActorRef = DeployService.createLocal(context, deployClient,
    containerService, monitoringService)

  private val cluster = Cluster(context.system)

  private val seedInstances: mutable.Set[InstanceInfo] = mutable.Set.empty
  private var numOfRequiredInstances: Int = numOfInstancesToJoin

  override def preStart(): Unit = {
    context.watch(containerService)
    context.watch(monitoringService)
    context.watch(deployService)
    monitoringService ! GetInstances()
    super.preStart()
  }

  private def stopServicesWithError(): Unit = {
    val error = MasterServiceException("Akkeeper Master Service fatal error")
    context.children.foreach(_ ! StopWithError(error))
  }

  private def finishInit(): Unit = {
    log.info("Master service successfully initialized")

    context.become(initializedReceive)
    unstashAll()
  }

  private def joinCluster(seedNodes: immutable.Seq[Address]): Unit = {
    cluster.joinSeedNodes(seedNodes)
    context.become(joinClusterReceive)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp])
  }

  private def serviceTerminatedReceive: Receive = {
    case Terminated(actor) =>
      if (actor == containerService) {
        log.error("Container Service has been terminated")
      } else if (actor == monitoringService) {
        log.error("Monitoring Service has been terminated")
      } else {
        log.error("Deploy Service has been terminated.")
      }
      if (context.children.isEmpty) {
        log.error("All services have been terminated. Shutting down the master")
        context.system.terminate()
      }
  }

  private def apiReceive: Receive = {
    case r: DeployContainer => deployService.forward(r)
    case r: InstanceRequest => monitoringService.forward(r)
    case r: ContainerRequest => containerService.forward(r)
    case TerminateMaster =>
      log.info("Master termination request has been received")
      Seq(containerService, monitoringService, deployService).foreach(context.stop)
  }

  private def clusterEventReceive: Receive = {
    case MemberUp(member) =>
      if (member.address == cluster.selfAddress) {
        log.debug("Master service successfully joined the cluster")
        cluster.unsubscribe(self)
        finishInit()
      }
  }

  private def apiStashReceive: Receive = {
    case _: WithRequestId => stash()
  }

  private def fetchInstancesListReceive: Receive = {
    case InstancesList(_, instanceIds) =>
      val nonMasterInstanceIds = instanceIds.filter(_.containerName != MasterServiceName)
      if (nonMasterInstanceIds.isEmpty) {
        log.info("No running instances were found. Creating a new Akka cluster")
        joinCluster(immutable.Seq(cluster.selfAddress))
      } else {
        log.info(s"Found ${nonMasterInstanceIds.size} running instances. Joining the existing Akka cluster")
        // Choose N random instances to join.
        val seedNodeIds = scala.util.Random.shuffle(nonMasterInstanceIds).take(numOfInstancesToJoin)
        numOfRequiredInstances = seedNodeIds.size
        seedNodeIds.foreach(monitoringService ! GetInstance(_))
      }

    case InstanceInfoResponse(_, info) =>
      seedInstances.add(info)
      log.debug(s"Received instance info. ${numOfRequiredInstances - seedInstances.size} " +
        "more needed to proceed")
      if (seedInstances.size >= numOfRequiredInstances) {
        val seedAddrs = immutable.Seq(seedInstances.map(_.address.get.address).toSeq: _*)
        joinCluster(cluster.selfAddress +: seedAddrs)
      }

    case other @ (_: InstanceResponse | _: OperationFailed) =>
      log.error(s"Failed to retrieve information about instances. Initialization failed: $other")
      stopServicesWithError()
  }

  // Initial state. Used to receive messages from MonitoringService to join the cluster.
  private def uninitializedReceive: Receive = {
    fetchInstancesListReceive orElse serviceTerminatedReceive orElse apiStashReceive
  }

  // Temporary state. Used while awaiting MemberUp event generated by a cluster leader.
  private def joinClusterReceive: Receive = {
    clusterEventReceive orElse serviceTerminatedReceive orElse apiStashReceive
  }

  // Actual "working" state. Forwards messages to corresponding actors.
  private def initializedReceive: Receive = {
    apiReceive orElse serviceTerminatedReceive
  }

  override def receive: Receive = uninitializedReceive
}

object MasterService extends RemoteServiceFactory {
  val MasterServiceName = "akkeeperMaster"

  override val actorName = MasterServiceName

  private[akkeeper] def createLocal(factory: ActorRefFactory, deployClient: DeployClient,
                                    instanceStorage: InstanceStorage.Async): ActorRef = {
    factory.actorOf(Props(classOf[MasterService], deployClient, instanceStorage), actorName)
  }
}
