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

import akka.actor.{ActorRef, ActorSystem, Address, Props, Terminated}
import akka.cluster.ClusterEvent.{MemberRemoved, ReachableMember}
import akka.cluster.{MemberStatus, UniqueAddress}
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper.ActorTestUtils
import akkeeper.api._
import akkeeper.storage.{InstanceStorage, RecordNotFoundException}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class MemberAutoDownServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("MemberAutoDownServiceSpec"))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def createMemberAutdownService(targetAddress: UniqueAddress,
                                         targetInstanceId: InstanceId,
                                         instanceStorage: InstanceStorage,
                                         pollInterval: FiniteDuration = 30 seconds): ActorRef = {
    childActorOf(Props(classOf[MemberAutoDownService], targetAddress,
      targetInstanceId, instanceStorage, pollInterval), s"autoDown-$targetInstanceId")
  }

  "A Member Auto Down Service" should "exclude a dead instance from the cluster" in {
    val port = 12345
    val address = UniqueAddress(Address("akka.tcp", "MemberAutoDownServiceSpec", "localhost", port), 1L)
    val instanceId = InstanceId("container")

    val storage = mock[InstanceStorage]
    (storage.getInstance _).expects(instanceId).returns(Future failed RecordNotFoundException(""))

    val service = createMemberAutdownService(address, instanceId, storage)
    watch(service)
    service ! MemberAutoDownService.PollInstanceStatus

    expectMsgClass(classOf[Terminated])
  }

  it should "periodically poll the instance status" in {
    val port = 12345
    val address = UniqueAddress(Address("akka.tcp", "MemberAutoDownServiceSpec", "localhost", port), 1L)
    val instanceId = InstanceId("container")
    val info = InstanceInfo(instanceId, InstanceUp, "", Set.empty, None, Set.empty)

    val storage = mock[InstanceStorage]
    (storage.getInstance _).expects(instanceId).returns(Future successful info).atLeastTwice()

    val service = createMemberAutdownService(address, instanceId, storage, 1 second)
    service ! MemberAutoDownService.PollInstanceStatus

    val timeout = 2000
    Thread.sleep(timeout)
    gracefulActorStop(service)
  }

  it should "stop when the target became reachable again" in {
    val port = 12345
    val address = UniqueAddress(Address("akka.tcp", "MemberAutoDownServiceSpec", "localhost", port), 1L)
    val member = createTestMember(address)
    val instanceId = InstanceId("container")
    val info = InstanceInfo(instanceId, InstanceUp, "", Set.empty, None, Set.empty)

    val storage = mock[InstanceStorage]
    (storage.getInstance _).expects(instanceId).returns(Future successful info)

    val service = createMemberAutdownService(address, instanceId, storage)
    watch(service)
    service ! MemberAutoDownService.PollInstanceStatus
    service ! ReachableMember(member)

    expectMsgClass(classOf[Terminated])
  }

  it should "stop when the target left the cluster" in {
    val port = 12345
    val address = UniqueAddress(Address("akka.tcp", "MemberAutoDownServiceSpec", "localhost", port), 1L)
    val member = createTestMember(address, MemberStatus.Removed)
    val instanceId = InstanceId("container")
    val info = InstanceInfo(instanceId, InstanceUp, "", Set.empty, None, Set.empty)

    val storage = mock[InstanceStorage]
    (storage.getInstance _).expects(instanceId).returns(Future successful info)

    val service = createMemberAutdownService(address, instanceId, storage)
    watch(service)
    service ! MemberAutoDownService.PollInstanceStatus
    service ! MemberRemoved(member, MemberStatus.exiting)

    expectMsgClass(classOf[Terminated])
  }

  it should "should retry on error" in {
    val port = 12345
    val address = UniqueAddress(Address("akka.tcp", "MemberAutoDownServiceSpec", "localhost", port), 1L)
    val instanceId = InstanceId("container")

    val storage = mock[InstanceStorage]
    (storage.getInstance _).expects(instanceId).returns(Future failed new Exception("")).atLeastTwice()

    val service = createMemberAutdownService(address, instanceId, storage, 1 second)
    service ! MemberAutoDownService.PollInstanceStatus

    val timeout = 2000
    Thread.sleep(timeout)
    gracefulActorStop(service)
  }

}
