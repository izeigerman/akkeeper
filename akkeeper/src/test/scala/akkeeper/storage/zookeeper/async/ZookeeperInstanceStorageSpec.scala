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
package akkeeper.storage.zookeeper.async

import akka.actor.Address
import akka.cluster.UniqueAddress
import akkeeper.AwaitMixin
import akkeeper.address._
import akkeeper.api._
import akkeeper.storage._
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import org.apache.curator.test.{TestingServer => ZookeeperServer}
import org.scalatest.{FlatSpec, Matchers}

class ZookeeperInstanceStorageSpec extends FlatSpec
  with Matchers with AwaitMixin with ZookeeperBaseSpec {

  private def withStorage[T](f: InstanceStorage => T): T = {
    val zookeeper = new ZookeeperServer()
    val connectionInterval = 1000
    val clientConfig = ZookeeperClientConfig(zookeeper.getConnectString, connectionInterval,
      1, None, "akkeeper")
    val storage = new ZookeeperInstanceStorage(clientConfig)
    withStorage[InstanceStorage, T](storage, zookeeper)(f)
  }

  private def createInstanceStatus(container: String): InstanceInfo = {
    val id = InstanceId(container)
    val port = 2550
    InstanceInfo(
      instanceId = id,
      status = InstanceUp,
      containerName = container,
      roles = Set("testRole"),
      address = Some(UniqueAddress(Address("akka.tcp", "AkkaSystem", "localhost", port), 1L)),
      actors = Set("/user/actor1")
    )
  }

  "A Zookeeper Instance Storage" should "return empty list of instances" in {
    withStorage { storage =>
      val instances = await(storage.getInstances)
      instances shouldBe empty

      val instancesByContainer = await(storage.getInstancesByContainer("container"))
      instancesByContainer shouldBe empty
    }
  }

  it should "register a new instance" in {
    withStorage { storage =>
      val container = "container1"
      val instance = createInstanceStatus(container)

      val instancesByContainer1 = await(storage.getInstancesByContainer("container1"))
      instancesByContainer1 shouldBe empty

      await(storage.registerInstance(instance)) shouldBe instance.instanceId
      val instancesByContainer2 = await(storage.getInstancesByContainer("container1"))
      instancesByContainer2.size shouldBe 1
      instancesByContainer2(0) shouldBe instance.instanceId

      await(storage.getInstances) should not be (empty)

      val instanceFromStorage = await(storage.getInstance(instance.instanceId))
      instanceFromStorage shouldBe instance
    }
  }

  it should "throw an error on register attempt if instance already exists" in {
    withStorage { storage =>
      val container = "container1"
      val instance = createInstanceStatus(container)

      await(storage.registerInstance(instance)) shouldBe instance.instanceId
      intercept[RecordAlreadyExistsException] {
        await(storage.registerInstance(instance))
      }
    }
  }

  it should "fetch multiple instances from different containers" in {
    withStorage { storage =>
      val container1 = "container1"
      val instance1 = createInstanceStatus(container1)

      val container2 = "container2"
      val instance2 = createInstanceStatus(container2)

      await(storage.registerInstance(instance1)) shouldBe instance1.instanceId
      await(storage.registerInstance(instance2)) shouldBe instance2.instanceId

      val instances = await(storage.getInstances)
      instances.size shouldBe 2

      instances should contain (instance1.instanceId)
      instances should contain (instance2.instanceId)
    }
  }

  it should "thrown an error if the instance with the given ID doesn't exist" in {
    withStorage { storage =>
      val id = InstanceId("container")
      intercept[RecordNotFoundException] {
        await(storage.getInstance(id))
      }
    }
  }
}
