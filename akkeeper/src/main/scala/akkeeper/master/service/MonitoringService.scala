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
import akka.cluster.{Cluster, MemberStatus, UniqueAddress}
import akka.cluster.ClusterEvent._
import akka.pattern.pipe
import akkeeper.api._
import akkeeper.common._
import akkeeper.common.api._
import akkeeper.config._
import akkeeper.container.service.ContainerInstanceService
import akkeeper.storage._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.concurrent.duration.FiniteDuration
import MonitoringService._

private[akkeeper] class MonitoringService(instanceStorage: InstanceStorage.Async)
  extends RequestTrackingService with Stash {

  private val instanceLaunchTimeout: FiniteDuration = context.system.settings.config.monitoring.launchTimeout

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
    initInstanceList()
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

  private def memberAutoDown(instanceId: InstanceId, instanceAddr: UniqueAddress): Unit = {
    log.debug(s"Launching the auto down job for instance ${instanceAddr.address}")
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
      case (_, Some(info)) if info.address.exists(_ == addr) => info
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
    if (instances.contains(instanceId)) {
      val instance = instances(instanceId)
      if (instance.exists(_.address.isDefined)) {
        // If the address is present we can use the local storage to retrieve
        // the information about this instance.
        instance.foreach(terminateInstance)
        sendAndRemoveOriginalSender(InstanceTerminated(requestId, instanceId))
      } else {
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
      }
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
    } else if (info.status != InstanceDeploying) {
      val updatedInfo =
        if (info.address.exists(a => !cluster.failureDetector.isAvailable(a.address))) {
          info.copy(status = InstanceUnreachable)
        } else {
          info
        }
      instances.put(info.instanceId, Some(updatedInfo))

      if (info.status == InstanceDeployFailed) {
        log.error(s"Instance ${info.instanceId} deployment failed")
        instances.remove(info.instanceId)
      } else if (info.status == InstanceLaunching) {
        val timeoutTask = context.system.scheduler.scheduleOnce(
          instanceLaunchTimeout, self,
          InstanceLaunchTimeout(info.instanceId))
        launchTimeoutTasks.put(info.instanceId, timeoutTask)
      } else {
        launchTimeoutTasks.get(info.instanceId).foreach(_.cancel())
        launchTimeoutTasks.remove(info.instanceId)
      }
    } else {
      instances.put(info.instanceId, Some(info))
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
    if (instances.contains(instanceId)) {
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
    val instanceIds = instances.keys.toSeq
    sender() ! InstancesList(requestId, instanceIds)
  }

  private def localGetInstancesBy(requestId: RequestId, roles: Set[String],
                                  containerName: String): Unit = {
    log.debug(s"Accessing the local cache to get instances (roles=$roles, containerName=$containerName)")
    val defaultFilter = (s: InstanceInfo) => true
    val containerFilter =
      if (containerName.nonEmpty) {
        s: InstanceInfo => s.containerName == containerName
      } else {
        defaultFilter
      }
    val roleFilter =
      if (roles.nonEmpty) {
        s: InstanceInfo => roles.intersect(s.roles).nonEmpty
      } else {
        defaultFilter
      }

    val searchResult = instances.collect {
      case (id, Some(info)) if containerFilter(info) && roleFilter(info) => id
    }.toSeq
    sender() ! InstancesList(requestId, searchResult)
  }

  private def storageGetInstancesBy(requestId: RequestId, roles: Set[String],
                                    containerName: String): Unit = {
    log.debug(s"Accessing the remote storage to get instances (roles=$roles, containerName=$containerName)")
    val localSelf = self
    val localSender = sender()

    val instancesFuture = instanceStorage.getInstancesByContainer(containerName)
    if (roles.nonEmpty) {
      instancesFuture.foreach(ids => {
        val infosFuture = Future.sequence(ids.map(id => instanceStorage.getInstance(id)))
        infosFuture.map(InstancesUpdate).pipeTo(localSelf)

        val response = infosFuture.map(infos => {
          val infoByRoles = infos.filter(s => roles.intersect(s.roles).nonEmpty)
          InstancesList(requestId, infoByRoles.map(_.instanceId))
        })
        response
          .recover { case NonFatal(e) => OperationFailed(requestId, e) }
          .pipeTo(localSender)
      })
    } else {
      instancesFuture.map(InstancesList(requestId, _)).pipeTo(localSender)
    }
  }

  private def onGetInstancesBy(request: GetInstancesBy): Unit = {
    val requestId = request.requestId
    val roles = request.roles
    val containerName = request.containerName.getOrElse("")

    if (instances.exists(_._2 eq None)) {
      // If we are missing information about at least one instance
      // we can't perform a proper local search.
      storageGetInstancesBy(requestId, roles, containerName)
    } else {
      localGetInstancesBy(requestId, roles, containerName)
    }
  }

  private def apiCommandReceive: Receive = {
    case request: GetInstance =>
      onGetInstance(request)
    case request: GetInstances =>
      onGetInstances(request)
    case request: GetInstancesBy =>
      onGetInstancesBy(request)
    case request: TerminateInstance =>
      onTerminateInstance(request)
  }

  private def internalEventReceive: Receive = {
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

  private def clusterEventReceive: Receive = {
    case UnreachableMember(member) =>
      if (member.status != MemberStatus.Exiting) {
        findInstanceByAddr(member.uniqueAddress)
          .foreach(i => {
            // Update the instance's status and launch an actor which will automatically
            // eliminate an unreachable member from the cluster.
            instances.put(i.instanceId, Some(i.copy(status = InstanceUnreachable)))
            memberAutoDown(i.instanceId, member.uniqueAddress)
          })
      } else {
        // Receiving the UNREACHABLE event for exiting members - is an expected behavior.
        cluster.down(member.address)
      }
    case ReachableMember(member) =>
      updateInstanceStatusByAddr(member.uniqueAddress, InstanceUp)
    case MemberUp(member) =>
      updateInstanceStatusByAddr(member.uniqueAddress, InstanceUp)
    case MemberRemoved(member, _) =>
      val info = findInstanceByAddr(member.uniqueAddress)
      info.foreach { i =>
        instances.remove(i.instanceId)
        launchTimeoutTasks.remove(i.instanceId)
        pendingTermination.remove(i.instanceId)
      }
  }

  private def uninitializedReceive: Receive = {
    case InitSuccessful(knownInstances) =>
      knownInstances.foreach(instances.put(_, None))
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberRemoved], classOf[UnreachableMember], classOf[ReachableMember])
      log.info("Monitoring service successfully initialized")
      become(initializedReceive)
      unstashAll()
    case InitFailed(reason) =>
      log.error(reason, "Monitoring service initialization failed")
      context.stop(self)
    case _: WithRequestId =>
      stash()
  }

  private def initializedReceive: Receive = {
    apiCommandReceive orElse internalEventReceive orElse clusterEventReceive
  }

  override def serviceReceive: Receive = {
    uninitializedReceive orElse internalEventReceive
  }
}

object MonitoringService extends RemoteServiceFactory {
  private case class InitSuccessful(knownInstances: Seq[InstanceId])
  private case class InitFailed(reason: Throwable)
  private case class InstanceTerminationFailed(instanceId: InstanceId, originalMsg: WithRequestId)
  private[akkeeper] case class InstancesUpdate(updates: Seq[InstanceInfo])
  private[akkeeper] case class InstanceLaunchTimeout(instanceId: InstanceId)

  override val actorName = "monitoringService"

  private[akkeeper] def createLocal(factory: ActorRefFactory,
                                    instanceStorage: InstanceStorage.Async): ActorRef = {
    factory.actorOf(Props(classOf[MonitoringService], instanceStorage), actorName)
  }
}
