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
import akka.cluster.{Cluster, Member, MemberStatus, UniqueAddress}
import akka.cluster.ClusterEvent._
import akka.pattern.{after, pipe}
import akkeeper.api._
import akkeeper.address._
import akkeeper.common._
import akkeeper.common.config._
import akkeeper.container.service.ContainerInstanceService
import akkeeper.storage._
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import MonitoringService._

private[akkeeper] class MonitoringService(instanceStorage: InstanceStorage)
  extends RequestTrackingService with Stash {

  private val config: Config = context.system.settings.config
  private val monitoringConfig: MonitoringConfig = config.monitoring
  private val akkeeperAkkaConfig: AkkeeperAkkaConfig = config.akkeeperAkka

  private implicit val dispatcher = context.dispatcher
  private val cluster = Cluster(context.system)
  private val instances: mutable.Map[InstanceId, Option[InstanceInfo]] = mutable.Map.empty
  private val pendingTermination: mutable.Map[InstanceId, RequestId] = mutable.Map.empty
  private val launchTimeoutTasks: mutable.Map[InstanceId, Cancellable] = mutable.Map.empty
  private val deadInstances: mutable.Set[InstanceId] = mutable.Set.empty

  override protected def trackedMessages: Set[Class[_]] = Set(classOf[TerminateInstance])

  override def preStart(): Unit = {
    log.debug("Initializing Monitoring service")
    instanceStorage.start()
    if (akkeeperAkkaConfig.useAkkaCluster) {
      initInstanceList()
    } else {
      finishInit()
    }
    super.preStart()
  }

  override def postStop(): Unit = {
    instanceStorage.stop()
    cluster.unsubscribe(self)
    super.postStop()
  }

  private def initInstanceList(): Unit = {
    instanceStorage.getInstances
      .map(InitSuccessful)
      .recover { case NonFatal(e) => InitFailed(e) }
      .pipeTo(self)
  }

  private def removeInstance(instanceId: InstanceId): Unit = {
    instances.remove(instanceId)
    launchTimeoutTasks.remove(instanceId)
    pendingTermination.remove(instanceId)
  }

  private def memberAutoDown(instanceId: InstanceId, instanceAddr: UniqueAddress): Unit = {
    log.info(s"Launching the auto down job for instance $instanceId (${instanceAddr.address})")
    val autoDownService = MemberAutoDownService.createLocal(context,
      instanceAddr, instanceId, instanceStorage)
    autoDownService ! MemberAutoDownService.PollInstanceStatus
  }

  private def onInstanceNotFound(requestId: RequestId, instanceId: InstanceId): Unit = {
    log.warning(s"Instance $instanceId was not found")
    sender() ! InstanceNotFound(requestId, instanceId)
  }

  private def findInstanceByAddr(addr: UniqueAddress): Option[InstanceInfo] = {
    instances.collectFirst {
      case (_, Some(info)) if info.address.exists(_ == toInstanceUniqueAddress(addr)) => info
    }
  }

  private def updateInstanceStatusByAddr(addr: UniqueAddress, status: InstanceStatus): Unit = {
    findInstanceByAddr(addr)
      .foreach(i => onInstanceInfoUpdate(i.copy(status = status)))
  }

  private def terminateInstance(instance: InstanceInfo): Unit = {
    val addr = instance.address.get
    val path = RootActorPath(addr.address) / "user" / ContainerInstanceService.ActorName
    val selection = context.actorSelection(path)
    selection ! StopInstance
    cluster.down(addr.address)
    log.info(s"Instance ${instance.instanceId} terminated successfully")
  }

  private def onTerminateInstance(request: TerminateInstance): Unit = {
    log.info(s"Terminating instance ${request.instanceId}")
    val instanceId = request.instanceId
    val requestId = request.requestId
    if (instances.get(instanceId).flatten.exists(_.address.isDefined)) {
      // If the address is present we can use the local storage to retrieve
      // the information about this instance.
      instances(instanceId).foreach(terminateInstance)
      sendAndRemoveOriginalSender(InstanceTerminated(requestId, instanceId))
    } else if (instances.contains(instanceId) || !akkeeperAkkaConfig.useAkkaCluster) {
      // Redirecting the information about the instance to ourselves
      // to terminate the instance later.
      pendingTermination.put(instanceId, requestId)
      instanceStorage.getInstance(instanceId)
        .recover {
          case _: RecordNotFoundException =>
            InstanceTerminationFailed(instanceId, InstanceNotFound(requestId, instanceId))
          case NonFatal(e) =>
            InstanceTerminationFailed(instanceId, OperationFailed(requestId, e))
        }
        .pipeTo(self)
    } else {
      onInstanceNotFound(requestId, instanceId)
    }
  }

  private def onInstanceInfoUpdate(info: InstanceInfo): Unit = {
    if (deadInstances.contains(info.instanceId)) {
      log.warning(s"Received update from the dead instance ${info.instanceId}. " +
        "Attempting to terminate this instance")
      if (info.address.nonEmpty) {
        terminateInstance(info)
      }
    } else {
      instances.put(info.instanceId, Some(info))

      if (info.status == InstanceDeployFailed) {
        log.error(s"Instance ${info.instanceId} deployment failed")
        instances.remove(info.instanceId)
      } else if (info.status == InstanceLaunching) {
        val timeoutTask = context.system.scheduler.scheduleOnce(
          monitoringConfig.launchTimeout, self,
          InstanceLaunchTimeout(info.instanceId))
        launchTimeoutTasks.put(info.instanceId, timeoutTask)
      } else if (info.status != InstanceDeploying) {
        launchTimeoutTasks.get(info.instanceId).foreach(_.cancel())
        launchTimeoutTasks.remove(info.instanceId)
        if (info.status == InstanceUp && !akkeeperAkkaConfig.useAkkaCluster) {
          // When Akka Cluster is disabled there is no way for us to receive events about
          // unavailability of launched members. This means that we can no longer rely
          // on the local registry. By removing launched instances from the local registry
          // we ensure that all API requests will be forced to access the remote storage
          // which always contains an up to date information.
          log.info(s"Instance ${info.instanceId} is up and running. Removing it from the local " +
            "storage due to inaccessibility of Akka Cluster")
          instances.remove(info.instanceId)
        }
      }
    }
    if (pendingTermination.contains(info.instanceId)) {
      // This instance is pending termination.
      log.info(s"Terminating a pending instance ${info.instanceId}")
      terminateInstance(info)
      val requestId = pendingTermination(info.instanceId)
      sendAndRemoveOriginalSender(InstanceTerminated(requestId, info.instanceId))
      pendingTermination.remove(info.instanceId)
    }
  }

  private def onGetInstance(request: GetInstance): Unit = {
    val instanceId = request.instanceId
    val requestId = request.requestId
    if (instances.contains(instanceId) || !akkeeperAkkaConfig.useAkkaCluster) {
      instances.get(instanceId).flatten match {
        case Some(info) =>
          log.debug(s"Accessing the local cache to get the instance $instanceId")
          sender() ! InstanceInfoResponse(requestId, info)
        case None =>
          log.debug(s"Accessing the remote storage to get the instance $instanceId")
          val future = instanceStorage.getInstance(instanceId)
          future.pipeTo(self)
          future
            .map(InstanceInfoResponse(requestId, _))
            .recover {
              case _: RecordNotFoundException => InstanceNotFound(requestId, instanceId)
              case NonFatal(e) => OperationFailed(requestId, e)
            }
            .pipeTo(sender())
      }
    } else {
      onInstanceNotFound(requestId, instanceId)
    }
  }

  private def onGetInstances(request: GetInstances): Unit = {
    val requestId = request.requestId
    if (akkeeperAkkaConfig.useAkkaCluster) {
      val instanceIds = instances.keySet.toSeq
      sender() ! InstancesList(requestId, instanceIds)
    } else {
      val localInstances = instances.values.flatten.map(_.instanceId).toSet
      instanceStorage.getInstances
        .map(remoteInstances => InstancesList(requestId, (localInstances ++ remoteInstances).toSeq))
        .pipeTo(sender())
    }
  }

  private def localGetInstancesBy(roles: Set[String],
                                  containerName: String,
                                  statuses: Set[InstanceStatus]): Future[Seq[InstanceId]] = {
    log.debug(s"Accessing the local cache to get instances (roles=$roles, containerName=$containerName)")
    val containerFilter: InstanceInfo => Boolean =
      if (containerName.nonEmpty) i => i.containerName == containerName else DefaultInstancesFilter
    val roleFilter: InstanceInfo => Boolean =
      if (roles.nonEmpty) i => roles.intersect(i.roles).nonEmpty else DefaultInstancesFilter
    val statusFilter: InstanceInfo => Boolean =
      if (statuses.nonEmpty) i => statuses.contains(i.status) else DefaultInstancesFilter

    val searchResult = instances.collect {
      case (id, Some(info)) if containerFilter(info) && roleFilter(info) && statusFilter(info) => id
    }.toSeq
    Future.successful(searchResult)
  }

  private def storageGetInstancesBy(roles: Set[String],
                                    containerName: String,
                                    statuses: Set[InstanceStatus]): Future[Seq[InstanceId]] = {
    log.debug(s"Accessing the remote storage to get instances (roles=$roles, containerName=$containerName)")
    val localSelf = self

    val instancesFuture = instanceStorage.getInstancesByContainer(containerName)
    if (roles.nonEmpty || statuses.nonEmpty) {
      instancesFuture.flatMap(ids => {
        val infosFuture = Future.sequence(ids.map(id => instanceStorage.getInstance(id)))
        infosFuture.map(InstancesUpdate).pipeTo(localSelf)

        infosFuture.map(infos => {
          val roleFilter: InstanceInfo => Boolean =
            if (roles.nonEmpty) i => roles.intersect(i.roles).nonEmpty else DefaultInstancesFilter
          val statusFilter: InstanceInfo => Boolean =
            if (statuses.nonEmpty) i => statuses.contains(i.status) else DefaultInstancesFilter
          val filteredInfos = infos.withFilter(roleFilter).withFilter(statusFilter)
          filteredInfos.map(_.instanceId)
        })
      })
    } else {
      instancesFuture
    }
  }

  private def onGetInstancesBy(request: GetInstancesBy): Unit = {
    val requestId = request.requestId
    val roles = request.roles
    val containerName = request.containerName.getOrElse("")
    val statuses = request.statuses

    val response =
      if (instances.exists(_._2 eq None) || !akkeeperAkkaConfig.useAkkaCluster) {
        // If we are missing information about at least one instance
        // we can't perform a purely local search.
        val localInstances = localGetInstancesBy(roles, containerName, statuses)
        val remoteInstances = storageGetInstancesBy(roles, containerName, statuses)
        for {
          local <- localInstances
          remote <- remoteInstances
        } yield (local.toSet ++ remote).toSeq
      } else {
        localGetInstancesBy(roles, containerName, statuses)
      }
    response
      .map(InstancesList(requestId, _))
      .recover { case NonFatal(e) => OperationFailed(requestId, e) }
      .pipeTo(sender())
  }

  private lazy val apiCommandReceive: Receive = {
    case request: GetInstance =>
      onGetInstance(request)
    case request: GetInstances =>
      onGetInstances(request)
    case request: GetInstancesBy =>
      onGetInstancesBy(request)
    case request: TerminateInstance =>
      onTerminateInstance(request)
  }

  private lazy val internalEventReceive: Receive = {
    case InstancesUpdate(update) =>
      update.foreach(onInstanceInfoUpdate)
    case info: InstanceInfo =>
      onInstanceInfoUpdate(info)
    case InstanceTerminationFailed(instanceId, original) =>
      log.warning(s"Instance $instanceId termination failed: $original")
      pendingTermination.remove(instanceId)
      sendAndRemoveOriginalSender(original)
    case Status.Failure(e) =>
      log.error(e, "Monitoring service unexpected error")
    case StopWithError(_) =>
      log.error("Stopping the Monitoring service because of external error")
      context.stop(self)
    case InstanceLaunchTimeout(instanceId) =>
      if (instances.get(instanceId).flatten.exists(_.status == InstanceLaunching)) {
        log.error(s"Launch timeout occurred for instance ${instanceId}")
        instances.remove(instanceId)
        deadInstances.add(instanceId)
      }
      launchTimeoutTasks.remove(instanceId)
  }

  private lazy val refreshInstancesListReceive: Receive = {
    case RefreshInstancesList(latestInstances) if instances.nonEmpty =>
      log.info("Refreshing the list of instances")
      val oldInstanceIds = Set.empty ++ instances.keySet
      instances.clear()
      latestInstances.foreach(instances.put(_, None))
      if (oldInstanceIds != instances.keySet && instances.nonEmpty) {
        log.info("There is a discrepancy between the local list of instances and one in the storage. " +
          "Additional refreshment is required")
        scheduleRefreshInstanceList()
      }
    case RefreshInstancesListFailed(e) =>
      log.error(e, "Failed to refresh the list of instances. Retrying...")
      scheduleRefreshInstanceList()
    case _: RefreshInstancesList =>
      log.info("Ignoring the instance list refreshment command")
  }

  private def clusterEventReceive: Receive = {
    case UnreachableMember(member) => onUnreachableMember(member)
    case ReachableMember(member) => onReachableMember(member)
    case MemberUp(member) => onMemberUp(member)
    case MemberRemoved(member, _) => onMemberRemoved(member)
  }

  private def onMemberUp(member: Member): Unit = {
    if (member.uniqueAddress == cluster.selfUniqueAddress) {
      if (instances.nonEmpty) {
        // Master could have missed some cluster events while trying to join the cluster,
        // so we need to bring the current list of instances in sync with ZooKeeper.
        scheduleRefreshInstanceList()
      }
    } else {
      updateInstanceStatusByAddr(member.uniqueAddress, InstanceUp)
    }
  }

  private def onMemberRemoved(member: Member): Unit = {
    findInstanceByAddr(member.uniqueAddress) match {
      case Some(i) =>
        log.info(s"Removing instance ${i.instanceId} (${member.address}) from the cluster")
        removeInstance(i.instanceId)
      case None =>
        log.warning(s"Unknown cluster member ${member.address} has been removed")
    }
  }

  private def onReachableMember(member: Member): Unit = {
    updateInstanceStatusByAddr(member.uniqueAddress, InstanceUp)
  }

  private def onUnreachableMember(member: Member): Unit = {
    if (member.status != MemberStatus.Exiting) {
      findInstanceByAddr(member.uniqueAddress) match {
        case Some(i) =>
          // Update the instance's status and launch an actor which will automatically
          // eliminate an unreachable member from the cluster.
          instances.put(i.instanceId, Some(i.copy(status = InstanceUnreachable)))
          memberAutoDown(i.instanceId, member.uniqueAddress)
        case None =>
          log.warning(s"Unknown unreachable cluster member ${member.address}. " +
            "Removing it from the cluster")
          cluster.down(member.address)
      }
    } else {
      // Receiving the UNREACHABLE event for exiting members - is an expected behavior.
      cluster.down(member.address)
    }
  }

  private def finishInit(): Unit = {
    log.info("Monitoring service successfully initialized")
    become(initializedReceive)
    unstashAll()
  }

  private def scheduleRefreshInstanceList(): Unit = {
    after(monitoringConfig.instanceListRefreshInterval,
      context.system.scheduler)(refreshInstancesList()).pipeTo(self)
  }

  private def refreshInstancesList(): Future[Any] = {
    instanceStorage.getInstances
      .map(RefreshInstancesList(_))
      .recover { case NonFatal(e) => RefreshInstancesListFailed(e) }
  }

  private lazy val uninitializedReceive: Receive = {
    case InitSuccessful(knownInstances) =>
      knownInstances.foreach(instances.put(_, None))
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberUp], classOf[MemberRemoved], classOf[UnreachableMember], classOf[ReachableMember])
      finishInit()
    case InitFailed(reason) =>
      log.error(reason, "Monitoring service initialization failed")
      context.stop(self)
    case _: WithRequestId =>
      stash()
  }

  private lazy val initializedReceive: Receive = {
    apiCommandReceive orElse internalEventReceive orElse
      refreshInstancesListReceive orElse clusterEventReceive
  }

  override lazy val serviceReceive: Receive = {
    uninitializedReceive orElse internalEventReceive
  }
}

object MonitoringService extends RemoteServiceFactory {
  private case class RefreshInstancesListFailed(e: Throwable)
  private case class InitSuccessful(knownInstances: Seq[InstanceId])
  private case class InitFailed(reason: Throwable)
  private case class InstanceTerminationFailed(instanceId: InstanceId, originalMsg: WithRequestId)
  private[akkeeper] case class InstancesUpdate(updates: Seq[InstanceInfo])
  private[akkeeper] case class InstanceLaunchTimeout(instanceId: InstanceId)

  private val DefaultInstancesFilter = (_: InstanceInfo) => true

  override val actorName = "monitoringService"

  private[akkeeper] def createLocal(factory: ActorRefFactory,
                                    instanceStorage: InstanceStorage): ActorRef = {
    factory.actorOf(Props(classOf[MonitoringService], instanceStorage), actorName)
  }
}
