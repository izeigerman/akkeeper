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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper._
import akkeeper.api._
import akkeeper.common._
import akkeeper.deploy._
import akkeeper.master.service.MonitoringService.InstancesUpdate
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.concurrent.Future

class DeployServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("DeployServiceSpec"))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def createContainer(name: String): ContainerDefinition = {
    val actor = ActorLaunchContext("name", "fqn")
    val cpus = 1
    val memory = 1024
    ContainerDefinition(name, cpus = cpus, memory = memory, actors = Seq(actor))
  }

  private def createDeployService(deployClient: DeployClient,
                                  containerService: ActorRef,
                                  monitoringService: ActorRef): ActorRef = {
    childActorOf(Props(classOf[DeployService], deployClient,
      containerService, monitoringService), DeployService.actorName)
  }

  "A Deploy Service" should "deploy container successfully" in {
    val container = createContainer("container")
    val expectedContainer = container.copy(
      jvmArgs = Seq("-Xms1G") ++ container.jvmArgs,
      jvmProperties = Map("property" -> "other_value"))

    val ids = (0 until 2).map(_ => InstanceId("container"))
    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    val deployResult = ids.map(id => Future successful DeploySuccessful(id))
    (deployClient.deploy _).expects(expectedContainer, *).returning(deployResult)

    val service = createDeployService(deployClient, self, self)
    val deployRequest = DeployContainer("container", 2,
      jvmArgs = Some(Seq("-Xms1G")),
      properties = Some(Map("property" -> "other_value")))
    service ! deployRequest

    val containerReq = expectMsgClass(classOf[GetContainer])
    containerReq.requestId shouldBe deployRequest.requestId
    containerReq.name shouldBe "container"

    service ! ContainerGetResult(containerReq.requestId, container)

    var instanceUpdates = 0
    var instanceInfos = 0
    var deployResponses = 0
    val expectPartial: PartialFunction[Any, Unit] = {
      case instancesUpdate: InstancesUpdate =>
        instancesUpdate.updates.size shouldBe 2
        instancesUpdate.updates.foreach(info => {
          info.status shouldBe InstanceDeploying
        })
        instanceUpdates += 1

      case instanceInfo: InstanceInfo =>
        instanceInfo.status shouldBe InstanceLaunching
        ids should contain (instanceInfo.instanceId)
        instanceInfos += 1

      case deployResponse: SubmittedInstances =>
        deployResponse.requestId shouldBe deployRequest.requestId
        deployResponse.containerName shouldBe "container"
        deployResponse.instanceIds.size shouldBe 2
        deployResponses += 1
    }

    // We expect for 4 messages in any order.
    for (_ <- 0 until 4) {
      expectMsgPF()(expectPartial)
    }
    instanceUpdates shouldBe 1
    instanceInfos shouldBe 2
    deployResponses shouldBe 1

    gracefulActorStop(service)
  }

  it should "return an error if the specified container is invalid" in {
    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()

    val service = createDeployService(deployClient, self, self)
    val deployRequest = DeployContainer("invalid", 1)
    service ! deployRequest

    val containerReq = expectMsgClass(classOf[GetContainer])
    containerReq.requestId shouldBe deployRequest.requestId
    containerReq.name shouldBe "invalid"

    val containerNotFound = ContainerNotFound(containerReq.requestId, "invalid")
    service ! containerNotFound

    expectMsg(containerNotFound)

    gracefulActorStop(service)
  }

  it should "fail the container deployment" in {
    val container = createContainer("container")
    val id = InstanceId("container")
    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    val deployResult = Future successful DeployFailed(id, new AkkeeperException(""))
    (deployClient.deploy _).expects(container, *).returning(Seq(deployResult))

    val service = createDeployService(deployClient, self, self)
    val deployRequest = DeployContainer("container", 1)
    service ! deployRequest

    val containerReq = expectMsgClass(classOf[GetContainer])
    containerReq.requestId shouldBe deployRequest.requestId
    containerReq.name shouldBe "container"

    service ! ContainerGetResult(containerReq.requestId, container)

    var instanceUpdates = 0
    var instanceInfos = 0
    var deployResponses = 0
    val expectPartial: PartialFunction[Any, Unit] = {
      case instancesUpdate: InstancesUpdate =>
        instancesUpdate.updates.size shouldBe 1
        instancesUpdate.updates.foreach(info => {
          info.status shouldBe InstanceDeploying
        })
        instanceUpdates += 1

      case instanceInfo: InstanceInfo =>
        instanceInfo.status shouldBe InstanceDeployFailed
        instanceInfo.instanceId shouldBe id
        instanceInfos += 1

      case deployResponse: SubmittedInstances =>
        deployResponse.requestId shouldBe deployRequest.requestId
        deployResponse.containerName shouldBe "container"
        deployResponse.instanceIds.size shouldBe 1
        deployResponses += 1
    }

    // We expect for 3 messages in any order.
    for (_ <- 0 until 3) {
      expectMsgPF()(expectPartial)
    }
    instanceUpdates shouldBe 1
    instanceInfos shouldBe 1
    deployResponses shouldBe 1

    gracefulActorStop(service)
  }

  it should "stop with an error" in {
    val exception = new AkkeeperException("fail")
    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.stopWithError _).expects(exception)

    val service = createDeployService(deployClient, self, self)
    service ! StopWithError(exception)

    service ! DeployContainer("container", 2)
    expectNoMessage()

    gracefulActorStop(service)
  }
}
