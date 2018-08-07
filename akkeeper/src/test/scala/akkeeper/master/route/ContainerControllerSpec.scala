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
package akkeeper.master.route

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import akkeeper.api._
import akkeeper.common.ContainerDefinition
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class ContainerControllerSpec(testSystem: ActorSystem) extends TestKit(testSystem)
  with FlatSpecLike with Matchers with ImplicitSender with RestTestUtils
  with DeployApiJsonProtocol with ContainerApiJsonProtocol with BeforeAndAfterAll {

  def this() = this(ActorSystem("ContainerControllerSpec"))

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  def createContainerDefinition(name: String): ContainerDefinition = {
    val memory = 1024
    val cpus = 1
    ContainerDefinition(name, cpus = cpus, memory = memory, actors = Seq.empty)
  }

  "Container Controller" should "return container by its ID" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val response = get[ContainerGetResult]("/containers/containerName", restPort)

      val request = expectMsgClass(classOf[GetContainer])
      request.name shouldBe "containerName"
      val getResult = ContainerGetResult(request.requestId,
        createContainerDefinition("containerName"))
      lastSender ! getResult

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe getResult
    }
  }

  it should "return 404 if the request container was not found" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val response = get[ContainerNotFound]("/containers/containerName", restPort)

      val request = expectMsgClass(classOf[GetContainer])
      request.name shouldBe "containerName"
      val notFound = ContainerNotFound(request.requestId, "containerName")
      lastSender ! notFound

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.NotFound.intValue
      actualResult shouldBe notFound
    }
  }

  it should "successfully update a container" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val container = createContainerDefinition("containerName")
      val response = patch[ContainerDefinition, ContainerUpdated](container,
        "/containers/containerName", restPort)

      val request = expectMsgClass(classOf[UpdateContainer])
      request.container shouldBe container
      val updated = ContainerUpdated(request.requestId, "containerName")
      lastSender ! updated

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe updated
    }
  }

  it should "successfully delete a container" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val response = delete[ContainerDeleted](
        "/containers/containerName", restPort)

      val request = expectMsgClass(classOf[DeleteContainer])
      request.name shouldBe "containerName"
      val deleted = ContainerDeleted(request.requestId, "containerName")
      lastSender ! deleted

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe deleted
    }
  }

  it should "return all available containers" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val response = get[ContainersList]("/containers/", restPort)

      val request = expectMsgClass(classOf[GetContainers])
      val list = ContainersList(request.requestId, Seq("containerName"))
      lastSender ! list

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe list
    }
  }

  it should "create a new container" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val container = createContainerDefinition("containerName")
      val response = post[ContainerDefinition, ContainerCreated](container,
        "/containers/", restPort)

      val request = expectMsgClass(classOf[CreateContainer])
      request.container shouldBe container
      val created = ContainerCreated(request.requestId, "containerName")
      lastSender ! created

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.Created.intValue
      actualResult shouldBe created
    }
  }

  it should "return an error if container already exists" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val container = createContainerDefinition("containerName")
      val response = post[ContainerDefinition, ContainerAlreadyExists](container,
        "/containers/", restPort)

      val request = expectMsgClass(classOf[CreateContainer])
      request.container shouldBe container
      val alreadyExists = ContainerAlreadyExists(request.requestId, "containerName")
      lastSender ! alreadyExists

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.Conflict.intValue
      actualResult shouldBe alreadyExists
    }
  }

  it should "fail if unexpected error occurred" in {
    val timeoutMilliseconds = 100
    implicit val timeout = Timeout(timeoutMilliseconds, TimeUnit.MILLISECONDS)
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val response = getRaw("/containers/", restPort)

      expectMsgClass(classOf[GetContainers])

      val (code, _) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
    }
  }

  it should "fail if the unexpected response arrives from the service" in {
    val controller = ContainerController(self)
    withHttpServer(controller.route) { restPort =>
      val response = getRaw("/containers/", restPort)

      val request = expectMsgClass(classOf[GetContainers])
      lastSender ! InstancesList(request.requestId, Seq.empty)

      val (code, _) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
    }
  }
}
