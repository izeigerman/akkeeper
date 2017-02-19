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
import akkeeper.storage._
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import org.apache.curator.test.{TestingServer => ZookeeperServer}
import org.apache.zookeeper.CreateMode
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AsyncZookeeperStorageSpec extends FlatSpec
  with Matchers with BeforeAndAfterAll with AwaitMixin {

  private val zookeeper = new ZookeeperServer()
  private val connectionInterval = 1000
  private val clientConfig = ZookeeperClientConfig(zookeeper.getConnectString,
    connectionInterval, 1, None, "akkeeper")

  override protected def afterAll(): Unit = {
    zookeeper.stop()
    super.afterAll()
  }

  private def withStorage[T](f: AsyncZookeeperClient => T): T = {
    val storage = new AsyncZookeeperClient(clientConfig, CreateMode.PERSISTENT)
    storage.start()
    try {
      f(storage)
    } finally {
      storage.stop()
    }
  }

  "An Async Zookeeper storage" should "create a new node" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      val payload = Array[Byte](1, 1, 2, 3)
      await(storage.create(node, payload))

      val readResult = await(storage.get(node))
      readResult shouldEqual payload
    }
  }

  it should "not create a node if it already exists" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      val payload = Array[Byte](1, 1, 2, 3)
      await(storage.create(node, payload))
      intercept[RecordAlreadyExistsException] {
        await(storage.create(node, payload))
      }
    }
  }

  it should "update a node successfully" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      await(storage.create(node))
      await(storage.get(node)) shouldBe empty

      val payload = Array[Byte](1, 1, 2, 3)
      await(storage.update(node, payload))
      await(storage.get(node)) shouldEqual payload
    }
  }

  it should "should throw an error on update attempt if node doesn't exist" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      val payload = Array[Byte](1, 1, 2, 3)
      intercept[RecordNotFoundException] {
        await(storage.update(node, payload))
      }
    }
  }

  it should "delete a node successfully" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      await(storage.create(node))
      await(storage.delete(node))
      intercept[RecordNotFoundException] {
        await(storage.get(node))
      }
    }
  }

  it should "throw an error on delete attempt if a node doesn't exist" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      intercept[RecordNotFoundException] {
        await(storage.delete(node))
      }
    }
  }

  it should "throw an error on get attempt if a node deosn't exist" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      intercept[RecordNotFoundException] {
        await(storage.get(node))
      }
    }
  }

  it should "retrieve node children successfully" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      await(storage.create(node))
      await(storage.create(node + "/child1"))
      await(storage.create(node + "/child2"))
      val result = await(storage.children(node)).toSet
      val expected = Set("child1", "child2")
      result shouldEqual expected
    }
  }

  it should "retrieve empty list of node children successfully" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      await(storage.create(node))
      val result = await(storage.children(node))
      result shouldBe empty
    }
  }

  it should "should check the node existence properly" in {
    withStorage { storage =>
      val node = UUID.randomUUID().toString
      intercept[RecordNotFoundException] {
        await(storage.exists(node))
      }
      await(storage.create(node))
      await(storage.exists(node))
    }
  }
}
