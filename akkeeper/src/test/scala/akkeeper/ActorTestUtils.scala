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
package akkeeper

import akka.actor.ActorRef
import akka.cluster.{Member, MemberStatus, UniqueAddress}
import akka.pattern.gracefulStop

import scala.concurrent.duration._

trait ActorTestUtils extends AwaitMixin {

  protected val gracefulStopTimeout: FiniteDuration = 6 seconds

  protected def gracefulActorStop(actor: ActorRef): Unit = {
    await(gracefulStop(actor, gracefulStopTimeout))
  }

  protected def createTestMember(addr: UniqueAddress): Member = {
    createTestMember(addr, MemberStatus.Up)
  }

  protected def createTestMember(addr: UniqueAddress, status: MemberStatus): Member = {
    createTestMember(addr, status, Set.empty)
  }

  protected def createTestMember(addr: UniqueAddress, status: MemberStatus, roles: Set[String]): Member = {
    val ctr = classOf[Member].getDeclaredConstructor(classOf[UniqueAddress], classOf[Int],
      classOf[MemberStatus], classOf[Set[String]])
    ctr.newInstance(addr, new Integer(1), status, roles + "dc-default")
  }
}
