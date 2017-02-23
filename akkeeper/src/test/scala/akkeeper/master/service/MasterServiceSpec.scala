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

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper.{AkkeeperException, ActorTestUtils}
import akkeeper.api._
import akkeeper.common.InstanceId
import akkeeper.deploy.{DeploySuccessful, DeployClient}
import akkeeper.storage.InstanceStorage
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import scala.concurrent.Future
import scala.concurrent.duration._
import MonitoringServiceSpec._

class MasterServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("MasterServiceSpec",
    ConfigFactory
      .load("application-container-test.conf")
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(new Integer(0)))))

  override def afterAll(): Unit = {
    system.shutdown()
    super.afterAll()
  }

  "A Master Service" should "initialize successfully and create a new cluster" in {
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)
    val deployClient = mock[DeployClient.Async]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.deploy _)
      .expects(*, *)
      .returns(Seq(Future successful DeploySuccessful(InstanceId("container1"))))

    val service = MasterService.createLocal(system, deployClient, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds shouldBe empty

    gracefulActorStop(service)
  }

  it should "initialize successfully and join the existing cluster" in {
    val selfAddr = Cluster(system).selfAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _).expects(instance.instanceId).returns(Future successful instance)

    val deployClient = mock[DeployClient.Async]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.deploy _)
      .expects(*, *)
      .returns(Seq(Future successful DeploySuccessful(InstanceId("container1"))))

    val service = MasterService.createLocal(system, deployClient, storage)

    val getInstances = GetInstances()
    service ! getInstances
    val response = expectMsgClass(classOf[InstancesList])
    response.requestId shouldBe getInstances.requestId
    response.instanceIds.size shouldBe 1

    gracefulActorStop(service)
  }

  it should "proxy deploy and container requests" in {
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val instanceIds = (0 until 2).map(_ => InstanceId("container1"))
    val deployFutures = instanceIds.map(id => Future successful DeploySuccessful(id))
    val deployClient = mock[DeployClient.Async]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.deploy _).expects(*, *).returns(deployFutures)
    (deployClient.deploy _)
      .expects(*, *)
      .returns(Seq(Future successful DeploySuccessful(InstanceId("container1"))))

    val service = MasterService.createLocal(system, deployClient, storage)

    val getContainers = GetContainers()
    service ! getContainers
    val container = expectMsgClass(classOf[ContainersList])
    container.requestId shouldBe getContainers.requestId
    container.containers.size shouldBe 2
    container.containers should contain allOf("container1", "container2")

    val deployContainer = DeployContainer("container1", 2)
    service ! deployContainer
    val deployResult = expectMsgClass(classOf[DeployedInstances])
    deployResult.requestId shouldBe deployContainer.requestId
    deployResult.containerName shouldBe "container1"
    deployResult.instanceIds.size shouldBe 2

    gracefulActorStop(service)
  }

  it should "shutdown the Actor system if the init process fails" in {
    val newSystem = ActorSystem("MasterServiceNegativeTest", system.settings.config)

    val selfAddr = Cluster(newSystem).selfAddress
    val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
    val storage = mock[InstanceStorage.Async]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
    (storage.getInstance _)
      .expects(instance.instanceId)
      .returns(Future failed new AkkeeperException(""))

    val deployClient = mock[DeployClient.Async]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.stopWithError _).expects(*)

    MasterService.createLocal(newSystem, deployClient, storage)

    newSystem.awaitTermination(3 seconds)
    newSystem.isTerminated shouldBe true
  }
}
