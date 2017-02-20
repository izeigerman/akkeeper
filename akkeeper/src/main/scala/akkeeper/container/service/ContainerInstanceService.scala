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
package akkeeper.container.service

import akka.actor._
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akka.pattern.pipe
import akka.cluster.Cluster
import akkeeper.api.OperationFailed
import akkeeper.common._
import akkeeper.master.service.MonitoringService
import akkeeper.storage.InstanceStorage
import scala.concurrent.duration._
import scala.util.control.NonFatal
import ContainerInstanceService._

class ContainerInstanceService(instanceStorage: InstanceStorage.Async,
                               instanceId: InstanceId,
                               masterAddress: Address,
                               retryInterval: FiniteDuration)
  extends Actor with ActorLogging {

  private implicit val dispatcher = context.dispatcher
  private val cluster = Cluster(context.system)
  private var thisInstance: Option[InstanceInfo] = None

  override def preStart(): Unit = {
    instanceStorage.start()
    super.preStart()
  }

  override def postStop(): Unit = {
    instanceStorage.stop()
    super.postStop()
  }

  private def launchActors(actors: Seq[ActorLaunchContext]): Unit = {
    actors.foreach(actor => {
      val clazz = Class.forName(actor.fqn)
      val actorRef = context.actorOf(Props(clazz), actor.name)
      context.watch(actorRef)
    })
  }

  private def notifyMonitoringService: Unit = {
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

  private def registerThisInstance: Unit = {
    if (!thisInstance.isDefined) {
      val actors = context.children.map(r => r.path.toStringWithoutAddress)
      val info = InstanceInfo(
        instanceId = instanceId,
        status = InstanceUp,
        containerName = instanceId.containerName,
        roles = cluster.selfRoles,
        address = Some(cluster.selfAddress),
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

  private def initializedReceive: Receive = {
    case id: InstanceId =>
      // The record was successfully saved to a storage.
      log.debug("Successfully registered this instance")
      notifyMonitoringService
    case OperationFailed(_, e) =>
      // Failed to save the record to a storage.
      log.error(e, s"Failed to store this instance information. Retrying in $retryInterval")
      // Scheduling retry.
      context.system.scheduler.scheduleOnce(retryInterval, self, RetryRegistration)
    case RetryRegistration =>
      log.info("Retrying instance registration process")
      registerThisInstance
  }

  private def joiningTheClusterReceive: Receive = {
    case MemberUp(member) =>
      if (member.address == cluster.selfAddress) {
        log.debug("Successfully joined the cluster")
        cluster.unsubscribe(self)
        context.become(initializedReceive)
        registerThisInstance
      }
  }

  private def waitingForActorsReceive: Receive = {
    case LaunchActors(actors) =>
      launchActors(actors)
      context.become(joiningTheClusterReceive)
      log.debug(s"Joining the cluster (master: $masterAddress)")
      cluster.join(masterAddress)
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp])
  }

  override def receive: Receive = waitingForActorsReceive
}

object ContainerInstanceService {
  private[akkeeper] case class LaunchActors(actors: Seq[ActorLaunchContext])
  private case object RetryRegistration
  private val DefaultRetryInterval = 30 seconds

  val ActorName = "akkeeperInstance"

  def createLocal(factory: ActorRefFactory,
                  instanceStorage: InstanceStorage.Async,
                  instanceId: InstanceId,
                  masterAddress: Address,
                  retryInterval: FiniteDuration = DefaultRetryInterval): ActorRef = {
    val props = Props(classOf[ContainerInstanceService], instanceStorage,
      instanceId, masterAddress, retryInterval)
    factory.actorOf(props, ActorName)
  }
}
