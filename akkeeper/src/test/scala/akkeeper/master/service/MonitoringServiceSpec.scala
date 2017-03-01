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

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, UniqueAddress, Member, MemberStatus}
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper._
import akkeeper.api._
import akkeeper.common._
import akkeeper.storage.InstanceStorage
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import scala.concurrent.Future
import MonitoringService._
import MonitoringServiceSpec._

class MonitoringServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("MonitoringServiceSpec"))

  override def afterAll(): Unit = {
    system.shutdown()
    super.afterAll()
  }

  private def createTestMember(addr: Address): Member = {
    val ctr = classOf[Member].getDeclaredConstructor(classOf[UniqueAddress], classOf[Int],
      classOf[MemberStatus], classOf[Set[String]])
    ctr.newInstance(UniqueAddress(addr, 1), new Integer(1), MemberStatus.Up, Set.empty[String])
  }

  "A Monitoring Service" should "not respond if it's not initialized" in {
    implicit val dispatcher = system.dispatcher
    val delayedResponse = Future {
      val sleepMs = 1000
      Thread.sleep(sleepMs)
      throw new AkkeeperException("")
    }
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(delayedResponse)

    val service = MonitoringService.createLocal(system, storage)

    service ! GetInstances()
    expectNoMsg()

    gracefulActorStop(service)
  }

  it should "become initialized successfully when there is no active instances" in {
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds shouldBe empty

    gracefulActorStop(service)
  }

  it should "become initialized successfully and return the instance info" in {
    val instance = createInstanceInfo()
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _).expects(instance.instanceId).returns(Future successful instance)

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds.size shouldBe 1
    response.instanceIds should contain (instance.instanceId)

    val getInstance = GetInstance(instance.instanceId)
    service ! getInstance

    val instanceResponse = expectMsgClass(classOf[InstanceInfoResponse])
    instanceResponse.requestId shouldBe getInstance.requestId
    instanceResponse.info shouldBe instance

    gracefulActorStop(service)
  }

  it should "respond properly if the instance is not found" in {
    val instance = createInstanceInfo()
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds.size shouldBe 1

    val getInstance = GetInstance(InstanceId("unknown"))
    service ! getInstance

    val instanceResponse = expectMsgClass(classOf[InstanceNotFound])
    instanceResponse.requestId shouldBe getInstance.requestId

    gracefulActorStop(service)
  }

  it should "respond properly if the remote storage error occurs" in {
    val instance = createInstanceInfo()
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future failed new AkkeeperException(""))

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds.size shouldBe 1

    val getInstance = GetInstance(instance.instanceId)
    service ! getInstance

    val instanceResponse = expectMsgClass(classOf[OperationFailed])
    instanceResponse.requestId shouldBe getInstance.requestId

    gracefulActorStop(service)
  }

  def testByContainerAndRoles(service: ActorRef, container: Option[String],
                              roles: Option[Set[String]],
                              expected: Set[InstanceInfo]): Unit = {
    val getInstancesBy = GetInstancesBy(roles = roles, containerName = container)
    service ! getInstancesBy

    val instancesResponse = expectMsgClass(classOf[InstancesList])
    instancesResponse.requestId shouldBe getInstancesBy.requestId
    instancesResponse.instanceIds.size shouldBe expected.size
    instancesResponse.instanceIds.foreach(id => {
      service ! GetInstance(id)
      val instanceInfo = expectMsgClass(classOf[InstanceInfoResponse])
      expected should contain (instanceInfo.info)
    })
  }

  it should "retrieve instances by role" in {
    val instance1 = createInstanceInfo()
    val instance2 = createInstanceInfo()
    val ids = Seq(instance1.instanceId, instance2.instanceId)

    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful ids)
    (storage.getInstancesByContainer _).expects("").returns(Future successful ids)
    (storage.getInstance _).expects(instance1.instanceId).returns(Future successful instance1)
    (storage.getInstance _).expects(instance2.instanceId).returns(Future successful instance2)

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 2

    // Testing 2 times to verify the local cache.
    for (_ <- 0 until 2) {
      testByContainerAndRoles(service, None, Some(Set("role")), Set(instance1, instance2))
    }

    gracefulActorStop(service)
  }

  it should "retrieve instances by container" in {
    val instance1 = createInstanceInfo("container1")
    val instance2 = createInstanceInfo("container2")
    val ids = Seq(instance1.instanceId, instance2.instanceId)

    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful ids)
    (storage.getInstancesByContainer _)
      .expects("container1")
      .returns(Future successful Seq(instance1.instanceId))
    (storage.getInstancesByContainer _)
      .expects("container2")
      .returns(Future successful Seq(instance2.instanceId))
    (storage.getInstance _).expects(instance1.instanceId).returns(Future successful instance1)
    (storage.getInstance _).expects(instance2.instanceId).returns(Future successful instance2)

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 2

    // Testing 2 times to verify the local cache.
    for (_ <- 0 until 2) {
      testByContainerAndRoles(service, Some("container1"), None, Set(instance1))
      testByContainerAndRoles(service, Some("container2"), None, Set(instance2))
    }
    testByContainerAndRoles(service, Some("invalid"), None, Set.empty)

    gracefulActorStop(service)
  }

  it should "retrieve instances by container and roles" in {
    val instance1 = createInstanceInfo("container1").copy(roles = Set("role11"))
    val instance2 = createInstanceInfo("container1").copy(roles = Set("role12"))
    val instance3 = createInstanceInfo("container2").copy(roles = Set("role21"))
    val instance4 = createInstanceInfo("container2").copy(roles = Set("role22"))
    val ids = Seq(instance1.instanceId, instance2.instanceId,
      instance3.instanceId, instance4.instanceId)

    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful ids)
    (storage.getInstancesByContainer _)
      .expects("container1")
      .returns(Future successful Seq(instance1.instanceId, instance2.instanceId))
      .atLeastTwice()
    (storage.getInstancesByContainer _)
      .expects("container2")
      .returns(Future successful Seq(instance3.instanceId, instance4.instanceId))
    (storage.getInstance _)
      .expects(instance1.instanceId)
      .returns(Future successful instance1)
      .atLeastTwice()
    (storage.getInstance _)
      .expects(instance2.instanceId)
      .returns(Future successful instance2)
      .atLeastTwice()
    (storage.getInstance _).expects(instance3.instanceId).returns(Future successful instance3)
    (storage.getInstance _).expects(instance4.instanceId).returns(Future successful instance4)

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 4

    // Testing 2 times to verify the local cache.
    for (_ <- 0 until 2) {
      testByContainerAndRoles(service, Some("container1"), Some(Set("role11")), Set(instance1))
      testByContainerAndRoles(service, Some("container1"), Some(Set("role12")), Set(instance2))
      testByContainerAndRoles(service, Some("container2"), Some(Set("role21")), Set(instance3))
      testByContainerAndRoles(service, Some("container2"), Some(Set("role22")), Set(instance4))
      testByContainerAndRoles(service, Some("container1"), None, Set(instance1, instance2))
      testByContainerAndRoles(service, Some("container2"), None, Set(instance3, instance4))
    }

    gracefulActorStop(service)
  }

  it should "retrieve instances by container (error case)" in {
    val instance1 = createInstanceInfo("container1")
    val instance2 = createInstanceInfo("container1")
    val ids = Seq(instance1.instanceId, instance2.instanceId)

    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful ids)
    (storage.getInstancesByContainer _)
      .expects("container1")
      .returns(Future successful Seq(instance1.instanceId, instance2.instanceId))
    (storage.getInstance _)
      .expects(instance1.instanceId)
      .returns(Future successful instance1)
    (storage.getInstance _)
      .expects(instance2.instanceId)
      .returns(Future failed new AkkeeperException(""))

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 2

    val getInstancesBy = GetInstancesBy(roles = Some(Set("role")),
      containerName = Some("container1"))
    service ! getInstancesBy

    expectMsgClass(classOf[OperationFailed])

    gracefulActorStop(service)
  }

  it should "terminate instance successfully (instance from storage)" in {
    val selfAddr = Cluster(system).selfAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future successful instance)

    val service = MonitoringService.createLocal(system, storage)

    val terminateRequest = TerminateInstance(instance.instanceId)
    service ! terminateRequest
    val terminateResponse = expectMsgClass(classOf[InstanceTerminated])
    terminateResponse.requestId shouldBe terminateRequest.requestId
    terminateResponse.instanceId shouldBe instance.instanceId

    gracefulActorStop(service)
  }

  it should "fail to terminate the instance (instance from local cache)" in {
    val selfAddr = Cluster(system).selfAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future successful instance)

    val service = MonitoringService.createLocal(system, storage)

    service ! GetInstance(instance.instanceId)
    val instanceResponse = expectMsgClass(classOf[InstanceInfoResponse])
    instanceResponse.info shouldBe instance

    val terminateRequest = TerminateInstance(instance.instanceId)
    service ! terminateRequest
    val terminateResponse = expectMsgClass(classOf[InstanceTerminated])
    terminateResponse.requestId shouldBe terminateRequest.requestId
    terminateResponse.instanceId shouldBe instance.instanceId

    gracefulActorStop(service)
  }

  it should "fail to terminate the instance" in {
    val selfAddr = Cluster(system).selfAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage.Async]
    val exception = new AkkeeperException("fail")
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future failed exception)

    val service = MonitoringService.createLocal(system, storage)

    val terminateRequest = TerminateInstance(instance.instanceId)
    service ! terminateRequest
    val terminateResponse = expectMsgClass(classOf[OperationFailed])
    terminateResponse.requestId shouldBe terminateRequest.requestId
    terminateResponse.cause shouldBe exception

    gracefulActorStop(service)
  }

  it should "update instance's info" in {
    val storage = mock[InstanceStorage.Async]
    val instanceId1 = InstanceId("container")
    val instance1 = InstanceInfo.deploying(instanceId1)
    val instanceId2 = InstanceId("container")
    val instance2 = InstanceInfo.deploying(instanceId2)
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = MonitoringService.createLocal(system, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds shouldBe empty

    service ! instance1
    service ! GetInstance(instanceId1)
    val instance1Response = expectMsgClass(classOf[InstanceInfoResponse])
    instance1Response.info shouldBe instance1

    service ! InstancesUpdate(Seq(instance2))
    service ! GetInstance(instanceId2)
    val instance2Response = expectMsgClass(classOf[InstanceInfoResponse])
    instance2Response.info shouldBe instance2

    gracefulActorStop(service)
  }

  it should "handle cluster events" in {
    val storage = mock[InstanceStorage.Async]

    val port = 12345
    val addr = Address("akka.tcp", system.name, "localhost", port)
    val member = createTestMember(addr)

    val instance = createInstanceInfo("container").copy(address = Some(addr))
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _)
      .expects()
      .returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future successful instance)

    val service = MonitoringService.createLocal(system, storage)

    service ! GetInstance(instance.instanceId)
    expectMsgClass(classOf[InstanceInfoResponse])

    service ! UnreachableMember(member)
    service ! GetInstance(instance.instanceId)
    val unreachableInfo = expectMsgClass(classOf[InstanceInfoResponse])
    unreachableInfo.info.status shouldBe InstanceUnreachable

    service ! ReachableMember(member)
    service ! GetInstance(instance.instanceId)
    val reachableInfo = expectMsgClass(classOf[InstanceInfoResponse])
    reachableInfo.info.status shouldBe InstanceUp

    service ! MemberRemoved(member.copy(MemberStatus.removed), MemberStatus.up)
    service ! GetInstance(instance.instanceId)
    expectMsgClass(classOf[InstanceNotFound])

    gracefulActorStop(service)
  }

  it should "stop with an error" in {
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = MonitoringService.createLocal(system, storage)

    service ! StopWithError(new AkkeeperException(""))

    service ! GetInstances()
    expectNoMsg()

    gracefulActorStop(service)
  }
}

object MonitoringServiceSpec {
  def createInstanceInfo(containerName: String = "container"): InstanceInfo = {
    InstanceInfo(InstanceId(containerName), InstanceDeploying, containerName,
      Set("role"), None, Set("/user/actor"))
  }
}
