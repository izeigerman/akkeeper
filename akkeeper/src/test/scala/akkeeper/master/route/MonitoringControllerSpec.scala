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
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class MonitoringControllerSpec(testSystem: ActorSystem) extends TestKit(testSystem)
  with FlatSpecLike with Matchers with ImplicitSender with RestTestUtils
  with MonitoringApiJsonProtocol with ContainerApiJsonProtocol with BeforeAndAfterAll {

  def this() = this(ActorSystem("MonitoringControllerSpec"))

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "Monitoring Controller" should "return status of the given instance" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val instanceId = InstanceId("container")
      val response = get[InstanceInfoResponse](s"/instances/$instanceId", restPort)

      val request = expectMsgClass(classOf[GetInstance])
      request.instanceId shouldBe instanceId
      val instanceInfo = InstanceInfoResponse(request.requestId, InstanceInfo.deploying(instanceId))
      lastSender ! instanceInfo

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe instanceInfo
    }
  }

  it should "return an error if the specified instance ID has invalid format" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val response = getRaw("/instances/invalid", restPort)

      expectNoMessage(1 second)

      val (code, _) = await(response)
      code shouldBe StatusCodes.BadRequest.intValue
    }
  }

  it should "return an error if the required instance was not found" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val instanceId = InstanceId("container")
      val response = get[InstanceNotFound](s"/instances/$instanceId", restPort)

      val request = expectMsgClass(classOf[GetInstance])
      request.instanceId shouldBe instanceId
      val instanceNotFound = InstanceNotFound(request.requestId, instanceId)
      lastSender ! instanceNotFound

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.NotFound.intValue
      actualResult shouldBe instanceNotFound
    }
  }

  it should "terminate the given instance" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val instanceId = InstanceId("container")
      val response = delete[InstanceTerminated](s"/instances/$instanceId", restPort)

      val request = expectMsgClass(classOf[TerminateInstance])
      request.instanceId shouldBe instanceId
      val instanceTerminated = InstanceTerminated(request.requestId, instanceId)
      lastSender ! instanceTerminated

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe instanceTerminated
    }
  }

  it should "return all available instances" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val instanceId = InstanceId("container")
      val response = get[InstancesList]("/instances/", restPort)

      val request = expectMsgClass(classOf[GetInstances])
      val instancesList = InstancesList(request.requestId, Seq(instanceId))
      lastSender ! instancesList

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe instancesList
    }
  }

  it should "return all available instances with specified roles, container and statuses" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val instanceId = InstanceId("container")
      val response = get[InstancesList](
        "/instances?role=role1&role=role2&containerName=container1&status=up&status=invalid",
        restPort)

      val request = expectMsgClass(classOf[GetInstancesBy])
      request.containerName shouldBe Some("container1")
      request.roles shouldBe Set("role1", "role2")
      request.statuses shouldBe Set(InstanceUp)
      val instancesList = InstancesList(request.requestId, Seq(instanceId))
      lastSender ! instancesList

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.OK.intValue
      actualResult shouldBe instancesList
    }
  }

  it should "fail if unexpected error occurred" in {
    val timeoutMilliseconds = 100
    implicit val timeout = Timeout(timeoutMilliseconds, TimeUnit.MILLISECONDS)
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val response = getRaw("/instances/", restPort)

      expectMsgClass(classOf[GetInstances])
      val (code, _) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
    }
  }

  it should "fail if the unexpected response arrives from the service" in {
    val controller = MonitoringController(self)
    withHttpServer(controller.route) { restPort =>
      val response = getRaw("/instances/", restPort)

      val request = expectMsgClass(classOf[GetInstances])
      lastSender ! SubmittedInstances(request.requestId, "container", Seq.empty)

      val (code, _) = await(response)
      code shouldBe StatusCodes.InternalServerError.intValue
    }
  }
}
