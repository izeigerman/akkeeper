/*
 * Copyright 2017-2018 Iaroslav Zeigerman
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

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes}
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper.api.TerminateMaster
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MasterControllerSpec(testSystem: ActorSystem) extends TestKit(testSystem)
  with FlatSpecLike with Matchers with ImplicitSender with RestTestUtils with BeforeAndAfterAll {

  def this() = this(ActorSystem("MasterControllerSpec"))

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "Master Controller" should "send master termination message" in {
    val controller = MasterController(self)
    withHttpServer(controller.route) { restPort =>
      val request = HttpRequest(uri = "/master/terminate")
        .withMethod(HttpMethods.POST)
      val response = sendRequest(request, restPort)

      expectMsg(TerminateMaster)

      val (code, actualResult) = await(response)
      code shouldBe StatusCodes.Accepted.intValue
      actualResult shouldBe empty
    }
  }
}
