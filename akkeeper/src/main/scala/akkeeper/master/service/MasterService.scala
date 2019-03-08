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
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp, MemberWeaklyUp}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import akkeeper.api._
import akkeeper.address._
import akkeeper.common.config._
import akkeeper.deploy.DeployClient
import akkeeper.master.service.MasterService._
import akkeeper.storage.InstanceStorage
import com.typesafe.config.Config

import scala.collection.{immutable, mutable}


private[akkeeper] class MasterService(deployClient: DeployClient,
                                      instanceStorage: InstanceStorage)
  extends Actor with ActorLogging with Stash {

  private val config: Config = context.system.settings.config
  private val akkeeperAkkaConfig: AkkeeperAkkaConfig = config.akkeeperAkka

  private implicit val dispatcher = context.dispatcher

  private val containerService: ActorRef = ContainerService.createLocal(context)
  private val monitoringService: ActorRef = MonitoringService.createLocal(context, instanceStorage)
  private val deployService: ActorRef = DeployService.createLocal(context, deployClient,
    containerService, monitoringService)

  private val heartbeatService: Option[ActorRef] =
    if (config.master.heartbeat.enabled) {
      Some(HeartbeatService.createLocal(context))
    } else {
      None
    }

  private val cluster = Cluster(context.system)

  private val seedInstances: mutable.Set[InstanceInfo] = mutable.Set.empty
  private var numOfRequiredInstances: Int = akkeeperAkkaConfig.seedNodesNum

  override def preStart(): Unit = {
    context.watch(containerService)
    context.watch(monitoringService)
    context.watch(deployService)
    heartbeatService.foreach(context.watch)

    if (akkeeperAkkaConfig.useAkkaCluster) {
      implicit val instanceListTimeout: Timeout = Timeout(config.master.instanceListTimeout)
      (monitoringService ? GetInstances())
        .map {
          case OperationFailed(_, error) => StopWithError(error)
          case other => other
        }
        .recover {
          case error => StopWithError(error)
        }
        .pipeTo(self)
    } else {
      finishInit()
    }
  }

  private def stopServicesWithError(e: Throwable): Unit = {
    log.error(e, "Terminating Akkeeper Master due to fatal error")
    val error = MasterServiceException(s"Akkeeper Master Service fatal error: ${e.getMessage}")
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
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[MemberWeaklyUp])
  }

  private def onMemberUp(member: Member): Unit = {
    if (member.address == cluster.selfAddress) {
      log.info("Master service successfully joined the cluster")
      cluster.unsubscribe(self)
      finishInit()
    }
  }

  private def scheduleClusterJoinTimeout(): Unit = {
    context.system.scheduler.scheduleOnce(akkeeperAkkaConfig.joinClusterTimeout, self, ClusterJoinTimeout)
  }

  private lazy val serviceTerminatedReceive: Receive = {
    case Terminated(actor) =>
      if (actor == containerService) {
        log.info("Container Service has been terminated")
      } else if (actor == monitoringService) {
        log.info("Monitoring Service has been terminated")
      } else if (actor == deployService) {
        log.info("Deploy Service has been terminated.")
      } else if (heartbeatService.exists(_ == actor)) {
        log.info("Heartbeat Service has been terminated.")
      }
      if (context.children.isEmpty) {
        log.info("All services have been terminated. Shutting down the master")
        context.system.terminate()
      }
  }

  private lazy val apiReceive: Receive = {
    case r: DeployContainer => deployService.forward(r)
    case r: InstanceRequest => monitoringService.forward(r)
    case r: ContainerRequest => containerService.forward(r)
    case Heartbeat => heartbeatService.foreach(_ ! Heartbeat)
    case TerminateMaster =>
      log.info("Master termination request has been received")
      Seq(containerService, monitoringService, deployService).foreach(context.stop)
      heartbeatService.foreach(context.stop)
  }

  private lazy val clusterEventReceive: Receive = {
    case MemberUp(member) => onMemberUp(member)
    case MemberWeaklyUp(member) => onMemberUp(member)
  }

  private lazy val clusterJoinTimeoutReceive: Receive = {
    case ClusterJoinTimeout =>
      log.error("Failed to join the existing cluster during " +
        s"${akkeeperAkkaConfig.joinClusterTimeout.toSeconds} seconds. Creating a new cluster")

      // We've just created a new cluster so we must reset the list of known instances.
      monitoringService ! RefreshInstancesList(Seq.empty)

      joinCluster(immutable.Seq(cluster.selfAddress))
  }

  private lazy val apiStashReceive: Receive = {
    case _: WithRequestId => stash()
  }

  private lazy val fetchInstancesListReceive: Receive = {
    case InstancesList(_, instanceIds) =>
      val nonMasterInstanceIds = instanceIds.filter(_.containerName != MasterServiceName)
      if (nonMasterInstanceIds.isEmpty) {
        log.info("No running instances were found. Creating a new Akka cluster")
        joinCluster(immutable.Seq(cluster.selfAddress))
      } else {
        log.info(s"Found ${nonMasterInstanceIds.size} running instances. Joining the existing Akka cluster")
        numOfRequiredInstances = Math.min(nonMasterInstanceIds.size, numOfRequiredInstances)
        nonMasterInstanceIds.foreach(monitoringService ! GetInstance(_))
        scheduleClusterJoinTimeout()
      }

    case InstanceInfoResponse(_, info) =>
      seedInstances.add(info)
      log.debug(s"Received instance info. ${numOfRequiredInstances - seedInstances.size} " +
        "more needed to proceed")
      if (seedInstances.size >= numOfRequiredInstances) {
        val seedAddrs = immutable.Seq(seedInstances.map(_.address.get.address).toSeq: _*)
        joinCluster(seedAddrs.map(toAkkaAddress))
      }

    case other @ (_: InstanceResponse | _: OperationFailed) =>
      log.error(s"Failed to retrieve information about instance ($other)")

    case StopWithError(e) =>
      stopServicesWithError(e)
  }

  // Initial state. Used to receive messages from MonitoringService to join the cluster.
  private lazy val uninitializedReceive: Receive = {
    fetchInstancesListReceive orElse serviceTerminatedReceive orElse
      apiStashReceive orElse clusterJoinTimeoutReceive
  }

  // Temporary state. Used while awaiting MemberUp event generated by a cluster leader.
  private lazy val joinClusterReceive: Receive = {
    clusterEventReceive orElse serviceTerminatedReceive orElse
      apiStashReceive orElse clusterJoinTimeoutReceive
  }

  // Actual "working" state. Forwards messages to corresponding actors.
  private lazy val initializedReceive: Receive = {
    apiReceive orElse serviceTerminatedReceive orElse ignoredInitEventsReceive
  }

  private lazy val ignoredInitEventsReceive: Receive = {
    case ClusterJoinTimeout =>
    case _: InstanceInfoResponse =>
  }

  override def receive: Receive = uninitializedReceive
}

object MasterService extends RemoteServiceFactory {
  val MasterServiceName = "akkeeperMaster"

  override val actorName = MasterServiceName

  private[akkeeper] def createLocal(factory: ActorRefFactory, deployClient: DeployClient,
                                    instanceStorage: InstanceStorage): ActorRef = {
    factory.actorOf(Props(classOf[MasterService], deployClient, instanceStorage), actorName)
  }

  private case object ClusterJoinTimeout
}
