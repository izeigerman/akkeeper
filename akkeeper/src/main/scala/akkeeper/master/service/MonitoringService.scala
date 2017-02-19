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
import akka.cluster.ClusterEvent._
import akka.pattern.pipe
import akkeeper.api._
import akkeeper.common._
import akkeeper.storage._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import MonitoringService._

private[akkeeper] class MonitoringService(instanceStorage: InstanceStorage.Async)
  extends RequestTrackingService with Stash {

  private implicit val dispatcher = context.dispatcher
  private val cluster = Cluster(context.system)
  private val instances: mutable.Map[InstanceId, Option[InstanceInfo]] = mutable.Map.empty

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

  private def findInstanceByAddr(addr: Address): Option[InstanceInfo] = {
    instances.collectFirst {
      case (id, Some(info)) if info.address.exists(_ == addr) => info
    }
  }

  private def updateInstanceStatusByAddr(addr: Address, status: InstanceStatus): Unit = {
    findInstanceByAddr(addr)
      .foreach(i => instances.put(i.instanceId, Some(i.copy(status = status))))
  }

  private def onInstanceInfoUpdate(info: InstanceInfo): Unit = {
    if (info.status != InstanceDeploying) {
      val updatedInfo =
        if (info.address.exists(!cluster.failureDetector.isAvailable(_))) {
          info.copy(status = InstanceUnreachable)
        } else {
          info
        }
      instances.put(info.instanceId, Some(updatedInfo))
    } else {
      instances.put(info.instanceId, Some(info))
    }
  }

  private def initInstanceList(): Unit = {
    instanceStorage.getInstances
      .map(InitSuccessful(_))
      .recover { case NonFatal(e) => InitFailed(e) }
      .pipeTo(self)
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
      log.warning(s"Instance $instanceId was not found")
      sender() ! InstanceNotFound(requestId, instanceId)
    }
  }

  private def onGetInstances(request: GetInstances): Unit = {
    val requestId = request.requestId
    val instanceIds = instances.keys.toSeq
    sender() ! InstancesList(requestId, instanceIds)
  }

  private def localGetInstancesBy(requestId: RequestId, roles: Set[String],
                                  container: String): Unit = {
    log.debug(s"Accessing the local cache to get instances (roles=$roles, container=$container)")
    val defaultFilter = (s: InstanceInfo) => true
    val containerFilter =
      if (container.nonEmpty) {
        (s: InstanceInfo) => s.containerName == container
      } else {
        defaultFilter
      }
    val roleFilter =
      if (roles.nonEmpty) {
        (s: InstanceInfo) => roles.intersect(s.roles).nonEmpty
      } else {
        defaultFilter
      }

    val searchResult = instances.collect {
      case (id, Some(info)) if containerFilter(info) && roleFilter(info) => id
    }.toSeq
    sender() ! InstancesList(requestId, searchResult)
  }

  private def storageGetInstancesBy(requestId: RequestId, roles: Set[String],
                                    container: String): Unit = {
    log.debug(s"Accessing the remote storage to get instances (roles=$roles, container=$container)")
    val localSelf = self
    val localSender = sender()

    val instancesFuture = instanceStorage.getInstancesByContainer(container)
    if (roles.nonEmpty) {
      instancesFuture.foreach(ids => {
        val infosFuture = Future.sequence(ids.map(id => instanceStorage.getInstance(id)))
        infosFuture.map(InstancesUpdate(_)).pipeTo(localSelf)

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
    val roles = request.roles.getOrElse(Set.empty)
    val containerName = request.containerName.getOrElse("")

    if (instances.exists(i => i._2 eq None)) {
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
      log.warning("Instance termination is not implemented")
      sender() ! OperationFailed(request.requestId,
        MasterServiceException("Instance termination is not implemented"))
  }

  private def internalEventReceive: Receive = {
    case InstancesUpdate(update) =>
      update.foreach(onInstanceInfoUpdate)
    case info: InstanceInfo =>
      onInstanceInfoUpdate(info)
    case Status.Failure(e) =>
      log.error(e, "Monitoring service unexpected error")
    case StopWithError(_) =>
      log.error("Stopping the Monitoring service because of external error")
      context.stop(self)
  }

  private def clusterEventReceive: Receive = {
    case UnreachableMember(member) =>
      updateInstanceStatusByAddr(member.address, InstanceUnreachable)
    case ReachableMember(member) =>
      updateInstanceStatusByAddr(member.address, InstanceUp)
    case MemberRemoved(member, _) =>
      val info = findInstanceByAddr(member.address)
      info.foreach(i => instances.remove(i.instanceId))
  }

  private def uninitializedReceive: Receive = {
    case InitSuccessful(knownInstances) =>
      knownInstances.foreach(id => instances.put(id, None))
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberRemoved], classOf[UnreachableMember], classOf[ReachableMember])
      log.info("Monitoring service successfully initialized")
      become(initializedReceive)
      unstashAll()
    case InitFailed(reason) =>
      log.error(reason, "Monitoring service initialization failed")
      context.stop(self)
    case other: WithRequestId =>
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
  private[akkeeper] case class InstancesUpdate(updates: Seq[InstanceInfo])

  override val actorName = "monitoringService"

  private[akkeeper] def createLocal(factory: ActorRefFactory,
                                    instanceStorage: InstanceStorage.Async): ActorRef = {
    factory.actorOf(Props(classOf[MonitoringService], instanceStorage), actorName)
  }
}
