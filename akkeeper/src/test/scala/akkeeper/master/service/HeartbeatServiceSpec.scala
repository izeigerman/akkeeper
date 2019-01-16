/*
 * Copyright 2017-2019 Iaroslav Zeigerman
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
import akkeeper.api.{Heartbeat, TerminateMaster}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class HeartbeatServiceSpec(system: ActorSystem) extends TestKit(system)
  with FlatSpecLike with Matchers with ImplicitSender with MockFactory with ActorTestUtils
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("HeartbeatServiceSpec", ConfigFactory.load()
    .withValue("akkeeper.master.heartbeat.interval", ConfigValueFactory.fromAnyRef("1s"))
    .withValue("akkeeper.master.heartbeat.missed-limit", ConfigValueFactory.fromAnyRef("2"))))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def createHeartbeatService(): ActorRef = {
    childActorOf(Props(classOf[HeartbeatService]), HeartbeatService.HeartbeatServiceName)
  }

  "Heartbeat Service" should "send termination message when heartbeat timeout occurs" in {
    val service = createHeartbeatService()
    expectMsg(3 seconds, TerminateMaster)

    gracefulActorStop(service)
  }

  it should "not terminate master if heartbeats are arriving as expected" in {
    val service = createHeartbeatService()
    val numOfHeartbeats = 4
    (0 until numOfHeartbeats).foreach { _ =>
      service ! Heartbeat
      expectNoMessage(1 second)
    }

    gracefulActorStop(service)
  }

  it should "tolerate one missed heartbeat" in {
    val service = createHeartbeatService()
    service ! Heartbeat
    expectNoMessage(2 seconds)
    service ! Heartbeat
    expectNoMessage(2 seconds)

    gracefulActorStop(service)
  }
}
