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
package akkeeper.common

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper.master.route.RestTestUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class ControllersSpec(testSystem: ActorSystem) extends TestKit(testSystem)
  with FlatSpecLike with Matchers with ImplicitSender with RestTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ControllerSpec"))

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "Composite Controller" should "compose multiple controllers" in {
    val controller1 = ControllersSpec.createTestController("test1", self)
    val controller2 = ControllersSpec.createTestController("test2", self)

    def testRoute(name: String, restPort: Int): Unit = {
      val response = getRaw(s"/api/v1/$name", restPort)
      val (code, result) = await(response)
      code shouldBe StatusCodes.OK.intValue
      result shouldBe name
    }

    val controller = ControllerComposite("api/v1", Seq(controller1, controller2))
    withHttpServer(controller.route) { restPort =>
      testRoute("test1", restPort)
      testRoute("test2", restPort)
    }
  }
}

object ControllersSpec extends Directives {
  private def createTestController(name: String, testService: ActorRef): RouteController = {
    new RouteController {
      val service = testService

      override val route: Route =
        path(name) {
          get {
            complete(StatusCodes.OK -> name)
          }
        }
    }
  }
}
