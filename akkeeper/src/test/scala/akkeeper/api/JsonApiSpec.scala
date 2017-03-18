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
package akkeeper.api

import akkeeper.AkkeeperException
import akkeeper.common._
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import ApiJsonProtocol._

class JsonApiSpec extends FlatSpec with Matchers {

  def testJson[T: JsonFormat](expected: T): Unit = {
    val jsonString = expected.toJson.compactPrint
    val actual = jsonString.parseJson.convertTo[T]
    actual shouldBe expected
  }

  "JSON API" should "(de)serialize Monitoring API" in {
    testJson(GetInstance(InstanceId("container")))
    testJson(GetInstances())
    testJson(GetInstancesBy(roles = Some(Set("role")), containerName = Some("container")))
    testJson(GetInstancesBy(roles = None, containerName = None))
    testJson(TerminateInstance(InstanceId("container")))
    testJson(InstanceInfoResponse(RequestId(), InstanceInfo.deploying(InstanceId("container"))))
    testJson(InstancesList(RequestId(), Seq(InstanceId("container"))))
    testJson(InstanceNotFound(RequestId(), InstanceId("container")))
    testJson(InstanceTerminated(RequestId(), InstanceId("container")))
  }

  it should "(de)serialize Deploy API" in {
    testJson(DeployContainer("container", 1, Seq("arg"), Map("prop" -> "value")))
    testJson(SubmittedInstances(RequestId(), "container", Seq(InstanceId("container"))))
  }

  it should "(de)serialize Container API" in {
    val memory = 1024
    val actorLaunchContext = ActorLaunchContext("name", "fqn")
    val definition = ContainerDefinition("container", 1, memory,
      Seq(actorLaunchContext), Seq("arg"), Map("prop" -> "value"),
      Map("envprop" -> "another_value"))

    testJson(CreateContainer(definition))
    testJson(UpdateContainer(definition))
    testJson(GetContainer("container"))
    testJson(GetContainers())
    testJson(DeleteContainer("container"))
    testJson(ContainersList(RequestId(), Seq("container")))
    testJson(ContainerGetResult(RequestId(), definition))
    testJson(ContainerNotFound(RequestId(), "container"))
    testJson(ContainerAlreadyExists(RequestId(), "container"))
    testJson(ContainerCreated(RequestId(), "container"))
    testJson(ContainerUpdated(RequestId(), "container"))
    testJson(ContainerDeleted(RequestId(), "container"))
  }

  it should "(de)serialize Common API" in {
    val operationFailed = OperationFailed(RequestId(), new AkkeeperException("fail"))
    val jsonPayload = operationFailed.toJson
    val actualMessage = jsonPayload.asJsObject.fields("message").convertTo[String]
    val actualRequestId = jsonPayload.asJsObject.fields("requestId").convertTo[String]
    actualMessage shouldBe "fail"
    actualRequestId shouldBe operationFailed.requestId.toString

  }
}
