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

import akka.actor.{ActorRef, ActorSystem, Address, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, MemberStatus, UniqueAddress}
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper._
import akkeeper.api._
import akkeeper.address._
import akkeeper.common._
import akkeeper.storage.{InstanceStorage, RecordNotFoundException}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.concurrent.Future
import MonitoringService._
import MonitoringServiceSpec._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

class MonitoringServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("MonitoringServiceSpec", ConfigFactory.load()
    .withValue("akkeeper.monitoring.launch-timeout", ConfigValueFactory.fromAnyRef("2s"))))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def createMonitoringService(instanceStorage: InstanceStorage): ActorRef = {
    childActorOf(Props(classOf[MonitoringService], instanceStorage), MonitoringService.actorName)
  }

  "A Monitoring Service" should "not respond if it's not initialized" in {
    implicit val dispatcher = system.dispatcher
    val delayedResponse = Future {
      val sleepMs = 1000
      Thread.sleep(sleepMs)
      throw new AkkeeperException("")
    }
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(delayedResponse)

    val service = createMonitoringService(storage)

    service ! GetInstances()
    expectNoMessage()

    gracefulActorStop(service)
  }

  it should "become initialized successfully when there is no active instances" in {
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = createMonitoringService(storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds shouldBe empty

    gracefulActorStop(service)
  }

  it should "become initialized successfully and return the instance info" in {
    val instance = createInstanceInfo()
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _).expects(instance.instanceId).returns(Future successful instance)

    val service = createMonitoringService(storage)

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
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))

    val service = createMonitoringService(storage)

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
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future failed new AkkeeperException(""))

    val service = createMonitoringService(storage)

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
                              roles: Option[Set[String]], statuses: Option[Set[InstanceStatus]],
                              expected: Set[InstanceInfo]): Unit = {
    val getInstancesBy = GetInstancesBy(roles = roles.getOrElse(Set.empty),
      containerName = container, statuses = statuses.getOrElse(Set.empty))
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

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful ids)
    (storage.getInstancesByContainer _).expects("").returns(Future successful ids)
    (storage.getInstance _).expects(instance1.instanceId).returns(Future successful instance1)
    (storage.getInstance _).expects(instance2.instanceId).returns(Future successful instance2)

    val service = createMonitoringService(storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 2

    // Testing 2 times to verify the local cache.
    for (_ <- 0 until 2) {
      testByContainerAndRoles(service, None, Some(Set("role")), None, Set(instance1, instance2))
    }

    gracefulActorStop(service)
  }

  it should "retrieve instances by container" in {
    val instance1 = createInstanceInfo("container1")
    val instance2 = createInstanceInfo("container2")
    val ids = Seq(instance1.instanceId, instance2.instanceId)

    val storage = mock[InstanceStorage]
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

    val service = createMonitoringService(storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 2

    // Testing 2 times to verify the local cache.
    for (_ <- 0 until 2) {
      testByContainerAndRoles(service, Some("container1"), None, None, Set(instance1))
      testByContainerAndRoles(service, Some("container2"), None, None, Set(instance2))
    }
    testByContainerAndRoles(service, Some("invalid"), None, None, Set.empty)

    gracefulActorStop(service)
  }

  it should "retrieve instances by container, roles and statuses" in {
    val instance1 = createInstanceInfo("container1").copy(roles = Set("role11"))
    val instance2 = createInstanceInfo("container1").copy(roles = Set("role12"))
    val instance3 = createInstanceInfo("container2").copy(roles = Set("role21"))
    val instance4 = createInstanceInfo("container2").copy(roles = Set("role22"))
    val instance5 = createInstanceInfo("container2").copy(roles = Set("role22"), status = InstanceUp)
    val ids = Seq(instance1.instanceId, instance2.instanceId,
      instance3.instanceId, instance4.instanceId, instance5.instanceId)

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful ids)
    (storage.getInstancesByContainer _)
      .expects("container1")
      .returns(Future successful Seq(instance1.instanceId, instance2.instanceId))
      .atLeastTwice()
    (storage.getInstancesByContainer _)
      .expects("container2")
      .returns(Future successful Seq(instance3.instanceId, instance4.instanceId, instance5.instanceId))
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
    (storage.getInstance _).expects(instance5.instanceId).returns(Future successful instance5)

    val service = createMonitoringService(storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 5

    // Testing 2 times to verify the local cache.
    for (_ <- 0 until 2) {
      // scalastyle:off line.size.limit
      testByContainerAndRoles(service, Some("container1"), Some(Set("role11")), None, Set(instance1))
      testByContainerAndRoles(service, Some("container1"), Some(Set("role12")), None, Set(instance2))
      testByContainerAndRoles(service, Some("container2"), Some(Set("role21")), None, Set(instance3))
      testByContainerAndRoles(service, Some("container2"), Some(Set("role22")), None, Set(instance4, instance5))
      testByContainerAndRoles(service, Some("container1"), None, None, Set(instance1, instance2))
      testByContainerAndRoles(service, Some("container2"), None, None, Set(instance3, instance4, instance5))
      testByContainerAndRoles(service, Some("container2"), None, Some(Set(InstanceUp)), Set(instance5))
      testByContainerAndRoles(service, Some("container2"), Some(Set("role22")), Some(Set(InstanceUp)), Set(instance5))
      testByContainerAndRoles(service, Some("container2"), Some(Set("role22")), Some(Set(InstanceUp, InstanceDeploying)), Set(instance4, instance5))
      testByContainerAndRoles(service, None, None, Some(Set(InstanceUp)), Set(instance5))
      testByContainerAndRoles(service, None, None, Some(Set(InstanceUnreachable)), Set.empty)
      // scalastyle:on line.size.limit
    }

    gracefulActorStop(service)
  }

  it should "retrieve instances by container (error case)" in {
    val instance1 = createInstanceInfo("container1")
    val instance2 = createInstanceInfo("container1")
    val ids = Seq(instance1.instanceId, instance2.instanceId)

    val storage = mock[InstanceStorage]
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

    val service = createMonitoringService(storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.instanceIds.size shouldBe 2

    val getInstancesBy = GetInstancesBy(roles = Set("role"),
      containerName = Some("container1"), statuses = Set(InstanceUp))
    service ! getInstancesBy

    expectMsgClass(classOf[OperationFailed])

    gracefulActorStop(service)
  }

  it should "terminate instance successfully (instance from storage)" in {
    val selfAddr = Cluster(system).selfUniqueAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future successful instance)

    val service = createMonitoringService(storage)

    val terminateRequest = TerminateInstance(instance.instanceId)
    service ! terminateRequest
    val terminateResponse = expectMsgClass(classOf[InstanceTerminated])
    terminateResponse.requestId shouldBe terminateRequest.requestId
    terminateResponse.instanceId shouldBe instance.instanceId

    gracefulActorStop(service)
  }

  it should "fail to terminate the instance (instance from local cache)" in {
    val selfAddr = Cluster(system).selfUniqueAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future successful instance)

    val service = createMonitoringService(storage)

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
    val selfAddr = Cluster(system).selfUniqueAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage]
    val exception = new AkkeeperException("fail")
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future failed exception)

    val service = createMonitoringService(storage)

    val terminateRequest = TerminateInstance(instance.instanceId)
    service ! terminateRequest
    val terminateResponse = expectMsgClass(classOf[OperationFailed])
    terminateResponse.requestId shouldBe terminateRequest.requestId
    terminateResponse.cause shouldBe exception

    gracefulActorStop(service)
  }

  it should "update instance's info" in {
    val storage = mock[InstanceStorage]
    val instanceId1 = InstanceId("container")
    val instance1 = InstanceInfo.deploying(instanceId1)
    val instanceId2 = InstanceId("container")
    val instance2 = InstanceInfo.deploying(instanceId2)
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = createMonitoringService(storage)

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
    val storage = mock[InstanceStorage]

    val uniqueAddr = Cluster(system).selfUniqueAddress
    val member = createTestMember(uniqueAddr)

    val instance = createInstanceInfo("container").copy(
      address = Some(uniqueAddr), status = InstanceUp)
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _)
      .expects()
      .returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future successful instance)
      .atLeastOnce()

    val service = createMonitoringService(storage)

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
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = createMonitoringService(storage)

    service ! StopWithError(new AkkeeperException(""))

    service ! GetInstances()
    expectNoMessage()

    gracefulActorStop(service)
  }

  it should "remove instances which hasn't been transitioned from the launching state" in {
    val storage = mock[InstanceStorage]
    val instanceId1 = InstanceId("container")
    val instance1 = InstanceInfo.launching(instanceId1)
    val instanceId2 = InstanceId("container")
    val instance2 = InstanceInfo.launching(instanceId2)
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = createMonitoringService(storage)

    service ! instance1
    service ! instance2

    service ! GetInstances()
    expectMsgClass(classOf[InstancesList])

    service ! instance1.copy(status = InstanceUp)

    val waitTimeout = 3000
    Thread.sleep(waitTimeout)

    service ! GetInstance(instanceId1)
    expectMsgClass(classOf[InstanceInfoResponse])

    service ! GetInstance(instanceId2)
    expectMsgClass(classOf[InstanceNotFound])

    gracefulActorStop(service)
  }

  it should "remove instances whose deployment has failed" in {
    val storage = mock[InstanceStorage]
    val instanceId1 = InstanceId("container")
    val instance1 = InstanceInfo.deploying(instanceId1)
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = createMonitoringService(storage)

    service ! instance1

    service ! GetInstances()
    expectMsgClass(classOf[InstancesList])

    service ! instance1.copy(status = InstanceDeployFailed)

    service ! GetInstance(instanceId1)
    expectMsgClass(classOf[InstanceNotFound])

    gracefulActorStop(service)
  }

  it should "terminate instance that has been considered dead previously" in {
    val storage = mock[InstanceStorage]
    val instanceId1 = InstanceId("container")
    val instance1 = InstanceInfo.launching(instanceId1)
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val service = createMonitoringService(storage)

    service ! instance1

    service ! GetInstances()
    expectMsgClass(classOf[InstancesList])

    val waitTimeout = 3000
    Thread.sleep(waitTimeout)

    service ! GetInstance(instanceId1)
    expectMsgClass(classOf[InstanceNotFound])

    val address = UniqueAddress(Address("akka.tcp", "MonitoringServiceSpec", "localhost", 2), 0L)
    service ! instance1.copy(status = InstanceUp, address = Some(address))
    service ! GetInstance(instanceId1)
    expectMsgClass(classOf[InstanceNotFound])

    gracefulActorStop(service)
  }

  it should "immediately remove instances which are not members of the cluster" in {
    val storage = mock[InstanceStorage]

    val port = 12345
    val addr = Address("akka.tcp", system.name, "localhost", port)
    val unknownUniqueAddr = UniqueAddress(addr, 1L)

    val instance = createInstanceInfo("container").copy(address = Some(unknownUniqueAddr))
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _)
      .expects()
      .returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future failed RecordNotFoundException(""))
      .anyNumberOfTimes()

    val service = createMonitoringService(storage)

    service ! instance
    service ! GetInstance(instance.instanceId)
    expectMsgClass(classOf[InstanceNotFound])

    gracefulActorStop(service)
  }
}

object MonitoringServiceSpec {
  def createInstanceInfo(containerName: String = "container"): InstanceInfo = {
    InstanceInfo(InstanceId(containerName), InstanceDeploying, containerName,
      Set("role"), None, Set("/user/actor"))
  }
}
