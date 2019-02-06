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
package akkeeper.container.service

import akka.actor._
import akka.pattern.pipe
import akka.cluster.{Cluster, Member}
import akkeeper.address._
import akkeeper.api._
import akkeeper.common._
import akkeeper.master.service.{MasterService, MonitoringService}
import akkeeper.storage.InstanceStorage

import scala.concurrent.duration._
import scala.collection.immutable
import scala.util.control.NonFatal
import ContainerInstanceService._
import akka.cluster.ClusterEvent._

class ContainerInstanceService(userActors: Seq[ActorLaunchContext],
                               instanceStorage: InstanceStorage,
                               instanceId: InstanceId,
                               masterAddress: Address,
                               registrationRetryInterval: FiniteDuration,
                               joinClusterTimeout: FiniteDuration)
  extends Actor with ActorLogging {

  private implicit val dispatcher = context.dispatcher
  private val cluster = Cluster(context.system)
  private var thisInstance: Option[InstanceInfo] = None

  private var instanceTerminationTask: Option[Cancellable] = None

  override def preStart(): Unit = {
    instanceStorage.start()
    self ! JoinCluster
  }

  override def postStop(): Unit = {
    instanceStorage.stop()
  }

  private def launchUserActors(): Unit = {
    userActors.foreach(actor => {
      log.debug(s"Deploying actor ${actor.name} (${actor.fqn})")
      val clazz = Class.forName(actor.fqn)
      val userActorRef = context.actorOf(Props(clazz), actor.name)
      context.watch(userActorRef)
    })
  }

  private def notifyMonitoringService(): Unit = {
    try {
      thisInstance.foreach(info => {
        val monitoringService = MonitoringService.createRemote(context.system)
        monitoringService ! info
        log.debug("Successfully reported to the Monitoring service")
      })
    } catch {
      case NonFatal(e) =>
        log.error(e, "Failed to notify the Monitoring service")
    }
  }

  private def registerThisInstance(): Unit = {
    if (!thisInstance.isDefined) {
      val actors = context.children.map(r => r.path.toStringWithoutAddress)
      val info = InstanceInfo(
        instanceId = instanceId,
        status = InstanceUp,
        containerName = instanceId.containerName,
        roles = cluster.selfRoles,
        address = Some(cluster.selfUniqueAddress),
        actors = actors.toSet
      )
      thisInstance = Some(info)
    }
    thisInstance.foreach(info => {
      instanceStorage.registerInstance(info)
        .recover {
          case NonFatal(e) => OperationFailed(RequestId(), e)
        }
        .pipeTo(self)
    })
  }

  private def terminateThisInstance(): Unit = {
    cluster.leave(cluster.selfAddress)
    CoordinatedShutdown(cluster.system).run(CoordinatedShutdown.ClusterLeavingReason)
  }

  private def scheduleInstanceTermination(): Unit = {
    val task = context.system.scheduler.scheduleOnce(joinClusterTimeout * 2, self, StopInstance)
    instanceTerminationTask = Some(task)
  }

  private def isAkkeeperMaster(member: Member): Boolean = {
    member.roles.contains(MasterService.MasterServiceName)
  }

  private def checkMasterIsUp(member: Member): Unit = {
    if (isAkkeeperMaster(member) && instanceTerminationTask.isDefined) {
      log.info("Akkeeper Master is back")
      instanceTerminationTask.foreach(_.cancel())
      instanceTerminationTask = None
    }
  }

  private def checkMasterIsDown(member: Member): Unit = {
    if (isAkkeeperMaster(member)) {
      log.warning("Akkeeper Master is unavailable. Scheduling the instance termination task")
      instanceTerminationTask.foreach(_.cancel())
      scheduleInstanceTermination()
    }
  }

  private lazy val initializedReceive: Receive = {
    case _: InstanceId =>
      // The record was successfully saved to a storage.
      log.debug("Successfully registered this instance")
      notifyMonitoringService()
    case OperationFailed(_, e) =>
      // Failed to save the record to a storage.
      log.error(e, "Failed to store this instance information. " +
        s"Retrying in $registrationRetryInterval")
      // Scheduling retry.
      context.system.scheduler.scheduleOnce(registrationRetryInterval,
        self, RetryRegistration)
    case RetryRegistration =>
      log.info("Retrying instance registration process")
      registerThisInstance()
    case StopInstance =>
      log.info("Termination command received. Stopping this instance")
      terminateThisInstance()
    case Terminated(_) =>
      if (context.children.isEmpty) {
        log.info("No running user actors left. Terminating this instance")
        terminateThisInstance()
      }
    case MemberUp(m) => checkMasterIsUp(m)
    case ReachableMember(m) => checkMasterIsUp(m)
    case UnreachableMember(m) => checkMasterIsDown(m)
    case MemberRemoved(m, _) => checkMasterIsDown(m)
    case JoinClusterTimeout =>
      // Safely ignore the timeout command.
  }

  private lazy val joiningClusterReceive: Receive = {
    case MemberUp(m) if m.uniqueAddress == cluster.selfUniqueAddress =>
      onClusterJoined(false)
    case MemberWeaklyUp(m) if m.uniqueAddress == cluster.selfUniqueAddress =>
      onClusterJoined(true)
    case JoinClusterTimeout =>
      onClusterJoinTimeout()
  }

  private def onClusterJoined(isWeakly: Boolean): Unit = {
    if (log.isInfoEnabled) {
      val weaklyMsg = if (isWeakly) " (weakly)" else ""
      log.info("Successfully joined the cluster" + weaklyMsg)
    }
    cluster.unsubscribe(self)
    launchUserActors()
    context.become(initializedReceive)
    registerThisInstance()

    // Subscribe for cluster events to detect when Akkeeper Master becomes unavailable.
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[UnreachableMember], classOf[MemberRemoved],
      classOf[MemberUp], classOf[ReachableMember])
  }

  private def onClusterJoinTimeout(): Unit = {
    log.error(s"Couldn't join the cluster during ${joinClusterTimeout.toSeconds} seconds. " +
      "Terminating this instance...")
    cluster.unsubscribe(self)
    terminateThisInstance()
  }

  private lazy val waitingForJoinCommandReceive: Receive = {
    case JoinCluster =>
      context.become(joiningClusterReceive)
      log.debug(s"Joining the cluster (master: $masterAddress)")
      cluster.joinSeedNodes(immutable.Seq(masterAddress))
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberUp], classOf[MemberWeaklyUp])
      // Scheduling a timeout command.
      context.system.scheduler.scheduleOnce(joinClusterTimeout, self, JoinClusterTimeout)
  }

  override def receive: Receive = waitingForJoinCommandReceive
}

object ContainerInstanceService {
  private case object JoinCluster
  private case object RetryRegistration
  private case object JoinClusterTimeout
  private[akkeeper] val DefaultRegistrationRetryInterval = 30 seconds
  private[akkeeper] val DefaultJoinClusterTimeout = 90 seconds
  private[akkeeper] val DefaultLeaveClusterTimeout = 30 seconds

  val ActorName = "akkeeperInstance"

  def createLocal(factory: ActorRefFactory,
                  userActors: Seq[ActorLaunchContext],
                  instanceStorage: InstanceStorage,
                  instanceId: InstanceId,
                  masterAddress: Address,
                  registrationRetryInterval: FiniteDuration = DefaultRegistrationRetryInterval,
                  joinClusterTimeout: FiniteDuration = DefaultJoinClusterTimeout): ActorRef = {
    val props = Props(classOf[ContainerInstanceService], userActors, instanceStorage,
      instanceId, masterAddress, registrationRetryInterval, joinClusterTimeout)
    factory.actorOf(props, ActorName)
  }
}
