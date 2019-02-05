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

import java.util.concurrent.TimeoutException

import akka.actor._
import akka.cluster.{Cluster, MemberStatus, UniqueAddress}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akkeeper.ActorTestUtils
import akkeeper.address._
import akkeeper.api._
import akkeeper.common.AkkeeperException
import akkeeper.common.config._
import akkeeper.master.service._
import akkeeper.storage.InstanceStorage
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import ContainerInstanceService._
import ContainerInstanceServiceSpec._
import TestUserActor._
import akka.cluster.ClusterEvent.{ReachableMember, UnreachableMember}

class ContainerInstanceServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ContainerInstanceServiceSpec",
    ConfigFactory.load().withMasterPort.withMasterRole))

  private val masterServiceMock: ActorRef = system.actorOf(
    Props(classOf[MasterServiceMock], self), MasterService.actorName)


  override protected def afterAll(): Unit = {
    gracefulActorStop(masterServiceMock)
    super.afterAll()
  }

  private def createExpectedInstanceInfo(instanceId: InstanceId,
                                         addr: UniqueAddress,
                                         actorPath: String = "/system/testActor-1/akkeeperInstance/testActor"
                                        ): InstanceInfo = {
    InstanceInfo(
      instanceId = instanceId,
      status = InstanceUp,
      containerName = instanceId.containerName,
      roles = Set("akkeeperMaster", "dc-default"),
      address = Some(addr),
      actors = Set(actorPath)
    )
  }

  private def createContainerInstanceService(userActors: Seq[ActorLaunchContext],
                                             instanceStorage: InstanceStorage,
                                             instanceId: InstanceId,
                                             masterAddress: Address,
                                             retryInterval: FiniteDuration = DefaultRegistrationRetryInterval,
                                             joinClusterTimeout: FiniteDuration = DefaultJoinClusterTimeout
                                            ): ActorRef = {
    val props = Props(classOf[ContainerInstanceService], userActors, instanceStorage,
      instanceId, masterAddress, retryInterval, joinClusterTimeout)
    childActorOf(props, ContainerInstanceService.ActorName)
  }

  "A Container Instance service" should "register itself successfully" in {
    val selfAddr = Cluster(system).selfUniqueAddress
    val instanceId = InstanceId("container")
    val expectedInstanceInfo = createExpectedInstanceInfo(instanceId, selfAddr)

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.registerInstance _)
      .expects(expectedInstanceInfo)
      .returns(Future successful instanceId)

    val actors = Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName))

    val service = createContainerInstanceService(actors, storage, instanceId, selfAddr.address)

    // Expect a notification from the Monitoring service mock.
    expectMsg(expectedInstanceInfo)

    // Verify that the user actor was actually launched.
    val userActor = system.actorSelection("/system/testActor-1/akkeeperInstance/testActor")
    userActor ! TestPing
    expectMsg(TestPong)

    gracefulActorStop(service)
  }

  it should "retry if the registration failed" in {
    val selfAddr = Cluster(system).selfUniqueAddress
    val instanceId = InstanceId("container")
    val expectedInstanceInfo = createExpectedInstanceInfo(instanceId, selfAddr)

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    val numberOfAttempts = 3
    (storage.registerInstance _)
      .expects(expectedInstanceInfo)
      .returns(Future failed new AkkeeperException("Registration failed"))
      .repeated(numberOfAttempts)

    val actors = Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName))

    val service = createContainerInstanceService(actors, storage,
      instanceId, selfAddr.address, retryInterval = 1 second)

    // No notification should arrive from the Monitoring service mock.
    val maxWaitForNoMsg = numberOfAttempts.seconds
    expectNoMessage(maxWaitForNoMsg)

    gracefulActorStop(service)
  }

  it should "terminate if the join timeout occurred" in {
    val newSystem = ActorSystem("ContainerInstanceServiceSpecTemp")
    val instanceId = InstanceId("container")

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()

    val actors = Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName))

    val seedPort = 12345
    ContainerInstanceService.createLocal(newSystem, actors, storage, instanceId,
      Address("akka.tcp", "ContainerInstanceServiceSpecTemp", "127.0.0.1", seedPort),
      joinClusterTimeout = 1 second)

    await(newSystem.whenTerminated)
  }

  it should "terminate this instance if all user actors have been terminated" in {
    val newSystem = ActorSystem("ContainerInstanceServiceSpecTemp",
      ConfigFactory.load().withMasterPort.withMasterRole)
    val newCluster = Cluster(newSystem)
    val instanceId = InstanceId("container")
    val expectedInstanceInfo = createExpectedInstanceInfo(instanceId,
      newCluster.selfUniqueAddress, actorPath = "/user/akkeeperInstance/testActor")

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.registerInstance _)
      .expects(expectedInstanceInfo)
      .returns(Future successful instanceId)

    val actors = Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName))

    ContainerInstanceService.createLocal(newSystem, actors, storage, instanceId, newCluster.selfAddress)

    // Verify that the user actor was actually launched.
    val testProbe = TestProbe()(newSystem)
    val userActor = newSystem.actorSelection("/user/akkeeperInstance/testActor")

    val joinPromise = Promise[Unit]
    newCluster.registerOnMemberUp(joinPromise.success(()))
    await(joinPromise.future)
    val waitTimeoutMs = 1000
    Thread.sleep(waitTimeoutMs)

    userActor.tell(TestPing, testProbe.ref)
    testProbe.expectMsg(TestPong)

    // Terminate the user actor.
    userActor ! TestTerminate

    val leavePromise = Promise[Unit]
    newCluster.registerOnMemberRemoved(leavePromise.success(()))
    await(leavePromise.future)

    await(newSystem.whenTerminated)
  }

  it should "terminate if the Akkeeper Master is unavailable" in {
    val newSystem = ActorSystem("ContainerInstanceServiceSpecTemp",
      ConfigFactory.load().withMasterPort.withMasterRole)
    val newCluster = Cluster(newSystem)
    val instanceId = InstanceId("container")

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.registerInstance _).expects(*).returns(Future successful instanceId)

    val actors = Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName))

    val service = ContainerInstanceService.createLocal(newSystem, actors, storage, instanceId,
      newCluster.selfAddress, joinClusterTimeout = 500 millis)

    // Verify that the user actor was actually launched.
    val testProbe = TestProbe()(newSystem)
    val userActor = newSystem.actorSelection("/user/akkeeperInstance/testActor")

    val joinPromise = Promise[Unit]
    newCluster.registerOnMemberUp(joinPromise.success(()))
    await(joinPromise.future)
    val waitTimeoutMs = 1000
    Thread.sleep(waitTimeoutMs)

    userActor.tell(TestPing, testProbe.ref)
    testProbe.expectMsg(TestPong)

    val akkeeperMasterMember = createTestMember(newCluster.selfUniqueAddress,
      MemberStatus.up, Set("akkeeperMaster"))

    testProbe.send(service, UnreachableMember(akkeeperMasterMember))

    await(newSystem.whenTerminated)
  }

  it should "detect that Akkeeper Master has become available again" in {
    val newSystem = ActorSystem("ContainerInstanceServiceSpecTemp",
      ConfigFactory.load().withMasterPort.withMasterRole)
    val newCluster = Cluster(newSystem)
    val instanceId = InstanceId("container")

    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.registerInstance _).expects(*).returns(Future successful instanceId)

    val actors = Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName))

    val service = ContainerInstanceService.createLocal(newSystem, actors, storage, instanceId,
      newCluster.selfAddress, joinClusterTimeout = 500 millis)

    // Verify that the user actor was actually launched.
    val testProbe = TestProbe()(newSystem)
    val userActor = newSystem.actorSelection("/user/akkeeperInstance/testActor")

    val joinPromise = Promise[Unit]
    newCluster.registerOnMemberUp(joinPromise.success(()))
    await(joinPromise.future)
    val waitTimeoutMs = 1000
    Thread.sleep(waitTimeoutMs)

    userActor.tell(TestPing, testProbe.ref)
    testProbe.expectMsg(TestPong)

    val akkeeperMasterMember = createTestMember(newCluster.selfUniqueAddress,
      MemberStatus.up, Set("akkeeperMaster"))

    testProbe.send(service, UnreachableMember(akkeeperMasterMember))
    testProbe.send(service, ReachableMember(akkeeperMasterMember))

    intercept[TimeoutException] {
      await(newSystem.whenTerminated)(3 seconds)
    }

    gracefulActorStop(service)
    newSystem.terminate()
  }
}

object ContainerInstanceServiceSpec {
  case object GetLastReceivedInfo

  class MasterServiceMock(callback: ActorRef) extends Actor {
    private val monitoringService = context.actorOf(
      Props(classOf[MonitoringServiceMock], callback), MonitoringService.actorName)

    override def receive: Receive = {
      case msg => monitoringService.forward(monitoringService)
    }
  }

  class MonitoringServiceMock(callback: ActorRef) extends Actor {
    private var lastReceivedInfo: Option[InstanceInfo] = None

    override def receive: Actor.Receive = {
      case expected: InstanceInfo =>
        lastReceivedInfo = Some(expected)
        callback ! expected
      case GetLastReceivedInfo =>
        lastReceivedInfo match {
          case Some(info) => sender() ! info
          case None => sender() ! Status.Failure(new Exception("No instance info received"))
        }
    }
  }
}
