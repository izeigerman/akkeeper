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
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper.{AkkeeperException, ActorTestUtils}
import akkeeper.api._
import akkeeper.common.ActorLaunchContext
import com.typesafe.config.ConfigFactory
import org.scalatest._

class ContainerServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with ActorTestUtils with BeforeAndAfterAll {

  def this() = this(ActorSystem("ContainerServiceSpec",
    ConfigFactory.load("application-container-test.conf")))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "A Container Service" should "return the list of available containers" in {
    val service = ContainerService.createLocal(system)

    val request = GetContainers()
    service ! request

    val response = expectMsgClass(classOf[ContainersList])
    response.requestId shouldBe request.requestId
    response.containers should contain allOf("container1", "container2")

    gracefulActorStop(service)
  }

  it should "return a container definition" in {
    val service = ContainerService.createLocal(system)

    val request1 = GetContainer("container1")
    service ! request1

    val response1 = expectMsgClass(classOf[ContainerGetResult])
    response1.requestId shouldBe request1.requestId
    val container1 = response1.container
    container1.name shouldBe "container1"
    container1.cpus shouldBe 1
    container1.memory shouldBe 1024
    container1.jvmArgs should contain ("-Xmx2G")
    container1.jvmProperties should contain ("property" -> "value")
    container1.environment shouldBe empty
    container1.actors.size shouldBe 2
    container1.actors should contain (ActorLaunchContext("actor1", "com.test.Actor1"))
    container1.actors should contain (ActorLaunchContext("actor2", "com.test.Actor2"))

    val request2 = GetContainer("container2")
    service ! request2

    val response2 = expectMsgClass(classOf[ContainerGetResult])
    response2.requestId shouldBe request2.requestId
    val container2 = response2.container
    container2.name shouldBe "container2"
    container2.cpus shouldBe 2
    container2.memory shouldBe 2048
    container2.jvmArgs shouldBe empty
    container2.jvmProperties shouldBe empty
    container2.environment should contain ("envProperty" -> "value")
    container2.actors.size shouldBe 1
    container2.actors should contain (ActorLaunchContext("actor3", "com.test.Actor3"))

    gracefulActorStop(service)
  }

  it should "not find a container" in {
    val service = ContainerService.createLocal(system)

    val request = GetContainer("invalid")
    service ! request
    expectMsg(ContainerNotFound(request.requestId, "invalid"))

    gracefulActorStop(service)
  }

  it should "delete a container" in {
    val service = ContainerService.createLocal(system)

    val deleteRequest = DeleteContainer("container1")
    service ! deleteRequest
    expectMsg(ContainerDeleted(deleteRequest.requestId, "container1"))

    val request = GetContainer("container1")
    service ! request
    expectMsg(ContainerNotFound(request.requestId, "container1"))

    gracefulActorStop(service)
  }

  it should "not delete a container that doesn't exist" in {
    val service = ContainerService.createLocal(system)

    val deleteRequest = DeleteContainer("invalid")
    service ! deleteRequest
    expectMsg(ContainerNotFound(deleteRequest.requestId, "invalid"))

    gracefulActorStop(service)
  }

  it should "update the existing container" in {
    val service = ContainerService.createLocal(system)

    val getRequest = GetContainer("container1")
    service ! getRequest

    val getResponse = expectMsgClass(classOf[ContainerGetResult])
    val container = getResponse.container

    val cpus = 5
    val updatedContainer = container.copy(cpus = cpus)
    val updateRequest = UpdateContainer(updatedContainer)
    service ! updateRequest
    expectMsg(ContainerUpdated(updateRequest.requestId, updatedContainer.name))

    val getRequest2 = GetContainer("container1")
    service ! getRequest2
    expectMsg(ContainerGetResult(getRequest2.requestId, updatedContainer))

    gracefulActorStop(service)
  }

  it should "not update the container that doesn't exist" in {
    val service = ContainerService.createLocal(system)

    val getRequest = GetContainer("container1")
    service ! getRequest

    val getResponse = expectMsgClass(classOf[ContainerGetResult])
    val container = getResponse.container

    val updatedContainer = container.copy(name = "invalid")
    val updateRequest = UpdateContainer(updatedContainer)
    service ! updateRequest
    expectMsg(ContainerNotFound(updateRequest.requestId, "invalid"))

    gracefulActorStop(service)
  }

  it should "should create a new container" in {
    val service = ContainerService.createLocal(system)

    val getRequest = GetContainer("container1")
    service ! getRequest

    val getResponse = expectMsgClass(classOf[ContainerGetResult])
    val container = getResponse.container

    val cpus = 5
    val newContainer = container.copy(name = "newContainer", cpus = cpus)
    val createRequest = CreateContainer(newContainer)
    service ! createRequest
    expectMsg(ContainerCreated(createRequest.requestId, "newContainer"))

    val getRequest2 = GetContainer("newContainer")
    service ! getRequest2
    expectMsg(ContainerGetResult(getRequest2.requestId, newContainer))

    gracefulActorStop(service)
  }

  it should "should not create a container which already exists" in {
    val service = ContainerService.createLocal(system)

    val getRequest = GetContainer("container1")
    service ! getRequest

    val getResponse = expectMsgClass(classOf[ContainerGetResult])
    val container = getResponse.container

    val createRequest = CreateContainer(container)
    service ! createRequest
    expectMsg(ContainerAlreadyExists(createRequest.requestId, "container1"))

    gracefulActorStop(service)
  }

  it should "stop with an error" in {
    val service = ContainerService.createLocal(system)

    service ! StopWithError(new AkkeeperException("fail"))
    service ! GetContainer("container1")
    expectNoMessage()

    gracefulActorStop(service)
  }
}
