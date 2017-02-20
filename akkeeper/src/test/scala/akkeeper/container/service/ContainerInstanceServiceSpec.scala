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
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper.{AkkeeperException, ActorTestUtils}
import akkeeper.common._
import akkeeper.master.service._
import akkeeper.storage.InstanceStorage
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.concurrent.Future
import scala.concurrent.duration._
import ContainerInstanceService._
import ContainerInstanceServiceSpec._
import TestUserActor._

class ContainerInstanceServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ContainerInstanceServiceSpec"))

  private def createExpectedInstanceInfo(instanceId: InstanceId,
                                         addr: Address): InstanceInfo = {
    InstanceInfo(
      instanceId = instanceId,
      status = InstanceUp,
      containerName = instanceId.containerName,
      roles = Set("akkeeperMaster"),
      address = Some(addr),
      actors = Set("/user/akkeeperInstance/testActor")
    )
  }

  "A Container Instance service" should "register itself successfully" in {
    val selfAddr = Cluster(system).selfAddress
    val instanceId = InstanceId("container")
    val expectedInstanceInfo = createExpectedInstanceInfo(instanceId, selfAddr)

    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.registerInstance _)
      .expects(expectedInstanceInfo)
      .returns(Future successful instanceId)

    val masterServiceMock = createMasterServiceMock(system, self)
    val service = ContainerInstanceService.createLocal(system, storage, instanceId, selfAddr)

    service ! LaunchActors(Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName)))

    // Expect a notification from the Monitoring service mock.
    expectMsg(expectedInstanceInfo)

    // Verify that the user actor was actually launched.
    val userActor = system.actorSelection("/user/akkeeperInstance/testActor")
    userActor ! TestPing
    expectMsg(TestPong)

    gracefulActorStop(service)
    gracefulActorStop(masterServiceMock)
  }

  it should "retry if the registration failed" in {
    val selfAddr = Cluster(system).selfAddress
    val instanceId = InstanceId("container")
    val expectedInstanceInfo = createExpectedInstanceInfo(instanceId, selfAddr)

    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    val numberOfAttempts = 3
    (storage.registerInstance _)
      .expects(expectedInstanceInfo)
      .returns(Future failed new AkkeeperException("Registration failed"))
      .repeated(numberOfAttempts)

    val masterServiceMock = createMasterServiceMock(system, self)
    val service = ContainerInstanceService.createLocal(system, storage,
      instanceId, selfAddr, retryInterval = 1 second)

    service ! LaunchActors(Seq(ActorLaunchContext("testActor", classOf[TestUserActor].getName)))

    // No notification should arrive from the Monitoring service mock.
    val maxWaitForNoMsg = numberOfAttempts.seconds
    expectNoMsg(maxWaitForNoMsg)

    gracefulActorStop(masterServiceMock)
    gracefulActorStop(service)
  }
}

object ContainerInstanceServiceSpec {
  def createMasterServiceMock(system: ActorSystem, callback: ActorRef): ActorRef = {
    system.actorOf(Props(classOf[MasterServiceMock], callback), MasterService.actorName)
  }

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
