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
import akkeeper.common.AkkeeperException
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class DeployControllerSpec(testSystem: ActorSystem) extends TestKit(testSystem)
  with FlatSpecLike with Matchers with ImplicitSender with RestTestUtils
  with DeployApiJsonProtocol with ContainerApiJsonProtocol with BeforeAndAfterAll {

  def this() = this(ActorSystem("DeployControllerSpec"))

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "Deploy Controller" should "handle deploy request properly" in {
    val controller = DeployController(self)
    withHttpServer(controller.route) { restPort =>
      val request = DeployContainer("container", 1)
      val response = post[DeployContainer, SubmittedInstances](request, "/deploy", restPort)

      expectMsg(request)
      val submittedInstances = SubmittedInstances(
        requestId = request.requestId,
        containerName = request.name,
        instanceIds = Seq(InstanceId(request.name)))
      lastSender ! submittedInstances

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.Accepted.intValue
      actualResult shouldBe submittedInstances
    }
  }

  it should "handle invalid container properly" in {
    val controller = DeployController(self)
    withHttpServer(controller.route) { restPort =>
      val request = DeployContainer("container", 1)
      val response = post[DeployContainer, ContainerNotFound](request, "/deploy", restPort)

      expectMsg(request)
      val notFound = ContainerNotFound(request.requestId, request.name)
      lastSender ! notFound

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.NotFound.intValue
      actualResult shouldBe notFound
    }
  }

  it should "handle deployment errors" in {
    val controller = DeployController(self)
    withHttpServer(controller.route) { restPort =>
      val request = DeployContainer("container", 1)
      val response = postRaw(request, "/deploy", restPort)

      expectMsg(request)
      val error = OperationFailed(request.requestId, new AkkeeperException("fail"))
      lastSender ! error

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
      actualResult should include("fail")
      actualResult should include(request.requestId.toString)
    }
  }

  it should "fail if unexpected error occurred" in {
    val timeoutMilliseconds = 100
    implicit val timeout = Timeout(timeoutMilliseconds, TimeUnit.MILLISECONDS)
    val controller = DeployController(self)
    withHttpServer(controller.route) { restPort =>
      val request = DeployContainer("container", 1)
      val response = postRaw(request, "/deploy", restPort)

      expectMsg(request)
      val (code, _) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
    }
  }

  it should "fail if the unexpected response arrives from the service" in {
    val controller = DeployController(self)
    withHttpServer(controller.route) { restPort =>
      val request = DeployContainer("container", 1)
      val response = postRaw(request, "/deploy", restPort)

      expectMsg(request)
      lastSender ! InstancesList(request.requestId, Seq.empty)

      val (code, _) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
    }
  }
}
