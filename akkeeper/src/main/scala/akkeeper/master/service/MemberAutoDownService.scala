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

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Status}
import akka.pattern.pipe
import akka.cluster.{Cluster, Member, UniqueAddress}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberRemoved, ReachableMember}
import akkeeper.common.{InstanceId, InstanceInfo}
import akkeeper.storage.{InstanceStorage, RecordNotFoundException}

import scala.concurrent.duration._
import MemberAutoDownService._

/**
  * Monitors a status of the specified unreachable instance and automatically
  * excludes it from the cluster if the instance was deregistered from the storage.
  * This actor terminates itself when the instance is excluded from the cluster or
  * when it becomes reachable again.
  *
  * @param targetAddress the address of the target instance.
  * @param targetInstanceId the ID of the target instance.
  * @param instanceStorage the instance storage.
  * @param pollInterval the finite interval which indicates how often
  *                     the instance status should be checked.
  */
class MemberAutoDownService(targetAddress: UniqueAddress,
                            targetInstanceId: InstanceId,
                            instanceStorage: InstanceStorage.Async,
                            pollInterval: FiniteDuration)
  extends Actor with ActorLogging {

  private implicit val dispatcher = context.dispatcher
  private val cluster: Cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberRemoved], classOf[ReachableMember])
  }

  override def receive: Receive = {
    pollCommandReceive orElse instanceStatusReceive orElse clusterEventReceive
  }

  private def clusterEventReceive: Receive = {
    case MemberRemoved(m, _) =>
      onMemberStatusUpdate(m, s"The instance $targetInstanceId has been removed from the cluster")
    case ReachableMember(m) =>
      onMemberStatusUpdate(m, s"The instance $targetInstanceId has become reachable again")
  }

  private def instanceStatusReceive: Receive = {
    case _: InstanceInfo =>
      log.info(s"The instance $targetInstanceId seems to be alive despite being unreachable")
      schedulePoll
    case Status.Failure(RecordNotFoundException(_)) =>
      log.warning(s"The instance $targetInstanceId was not found in the storage. " +
        "Excluding it from the cluster")
      cluster.down(targetAddress.address)
      cluster.unsubscribe(self)
      context.stop(self)
    case Status.Failure(e) =>
      log.error(e, s"Failed to retrieve a status of the instance $targetInstanceId. Retrying...")
      schedulePoll
  }

  private def pollCommandReceive: Receive = {
    case PollInstanceStatus =>
      instanceStorage.getInstance(targetInstanceId).pipeTo(self)
  }

  private def onMemberStatusUpdate(member: Member, logMsg: => String): Unit = {
    if (member.uniqueAddress == targetAddress) {
      log.info(logMsg)
      cluster.unsubscribe(self)
      context.stop(self)
    }
  }

  private def schedulePoll: Unit = {
    context.system.scheduler.scheduleOnce(pollInterval, self, PollInstanceStatus)
  }
}

object MemberAutoDownService {
  private[akkeeper] case object PollInstanceStatus

  private val DefaultPollInterval = 30 seconds

  private[akkeeper] def createLocal(factory: ActorRefFactory,
                                    targetAddress: UniqueAddress,
                                    targetInstanceId: InstanceId,
                                    instanceStorage: InstanceStorage.Async,
                                    pollInterval: FiniteDuration = DefaultPollInterval): ActorRef = {
    factory.actorOf(Props(classOf[MasterService], targetAddress,
      targetInstanceId, instanceStorage, pollInterval), s"autoDown-$targetInstanceId")
  }
}
