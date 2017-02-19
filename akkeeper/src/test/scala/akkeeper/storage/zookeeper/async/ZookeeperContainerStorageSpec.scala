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

import java.util.UUID

import akkeeper.AwaitMixin
import akkeeper.common.ContainerDefinition
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import akkeeper.storage._
import org.apache.curator.test.{TestingServer => ZookeeperServer}
import org.scalatest.{FlatSpec, Matchers}

class ZookeeperContainerStorageSpec extends FlatSpec
  with Matchers with AwaitMixin with ZookeeperBaseSpec {

  private def withStorage[T](f: ContainerStorage.Async => T): T = {
    val zookeeper = new ZookeeperServer()
    val connectionInterval = 1000
    val clientConfig = ZookeeperClientConfig(zookeeper.getConnectString, connectionInterval,
      1, None, "akkeeper")
    val storage = new ZookeeperContainerStorage(clientConfig)
    withStorage[ContainerStorage.Async, T](storage, zookeeper)(f)
  }

  private def createTestContainer(name: String): ContainerDefinition = {
    val memory = 1024
    ContainerDefinition(name = name, cpus = 1, memory = memory, actors = Seq.empty)
  }

  "A Zookeeper Container Storage" should "create a new container properly" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      val container = createTestContainer(containerName)
      await(storage.createContainer(container)) should include (containerName)
      await(storage.getContainer(containerName)) shouldEqual container
    }
  }

  it should "throw an exception on create attempt if container already exists" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      val container = createTestContainer(containerName)
      await(storage.createContainer(container))
      intercept[RecordAlreadyExistsException] {
        await(storage.createContainer(container))
      }
    }
  }

  it should "should update container properly" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      val container = createTestContainer(containerName)
      await(storage.createContainer(container)) should include (containerName)
      await(storage.getContainer(containerName)) shouldEqual container

      val updatedContainer = container.copy(cpus = 2)
      await(storage.updateContainer(updatedContainer)) should include (containerName)
      await(storage.getContainer(containerName)) shouldEqual updatedContainer
    }
  }

  it should "throw an exception on update attempt if container doesn't exist" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      val container = createTestContainer(containerName)
      intercept[RecordNotFoundException] {
        await(storage.updateContainer(container))
      }
    }
  }

  it should "retrieve all available containers" in {
    withStorage { storage =>
      await(storage.getContainers) should be (empty)

      val containerName = UUID.randomUUID().toString
      val container = createTestContainer(containerName)
      await(storage.createContainer(container)) should include (containerName)
      val containers = await(storage.getContainers)
      containers should not be (empty)
      containers should contain (containerName)
    }
  }

  it should "delete a node properly" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      val container = createTestContainer(containerName)
      await(storage.createContainer(container)) should include (containerName)
      await(storage.getContainer(containerName)) shouldEqual container

      await(storage.deleteContainer(containerName)) should include (containerName)
    }
  }

  it should "throw an exception if there is nothing to delete" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      intercept[RecordNotFoundException] {
        await(storage.deleteContainer(containerName))
      }
    }
  }

  it should "throw an exception on get attempt if container doesn't exist" in {
    withStorage { storage =>
      val containerName = UUID.randomUUID().toString
      intercept[RecordNotFoundException] {
        await(storage.getContainer(containerName))
      }
    }
  }
}
