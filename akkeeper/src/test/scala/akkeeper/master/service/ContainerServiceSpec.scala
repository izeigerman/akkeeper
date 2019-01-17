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
import akkeeper.ActorTestUtils
import akkeeper.api._
import akkeeper.common.AkkeeperException
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

  private def createContainerService: ActorRef = {
    childActorOf(Props(classOf[ContainerService]), ContainerService.actorName)
  }

  "A Container Service" should "return the list of available containers" in {
    val service = createContainerService

    val request = GetContainers()
    service ! request

    val response = expectMsgClass(classOf[ContainersList])
    response.requestId shouldBe request.requestId
    response.containers should contain allOf("container1", "container2")

    gracefulActorStop(service)
  }

  it should "return a container definition" in {
    val service = createContainerService

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
    val service = createContainerService

    val request = GetContainer("invalid")
    service ! request
    expectMsg(ContainerNotFound(request.requestId, "invalid"))

    gracefulActorStop(service)
  }

  it should "stop with an error" in {
    val service = createContainerService

    service ! StopWithError(new AkkeeperException("fail"))
    service ! GetContainer("container1")
    expectNoMessage()

    gracefulActorStop(service)
  }
}
