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
package akkeeper.storage.zookeeper

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class ZookeeperClientConfigSpec extends FlatSpec with Matchers {

  val appConfig = ConfigFactory.parseString(
    """
      |zookeeper {
      |  servers = "localhost:2180"
      |  connection-interval-ms = 3000
      |  max-retries = 10
      |  namespace = "akkeeper"
      |  can-be-readonly = true
      |  client-threads = 2
      |}
    """.stripMargin)

  "Zookeeper Client Config" should "derive properties from application config" in {
    val zkConfig = ZookeeperClientConfig.fromConfig(appConfig.getConfig("zookeeper"))
    zkConfig.servers shouldBe "localhost:2180"
    val interval = 3000
    zkConfig.connectionIntervalMs shouldBe interval
    val retries = 10
    zkConfig.maxRetries shouldBe retries
    zkConfig.namespace shouldBe "akkeeper"
    zkConfig.canBeReadOnly shouldBe true
    zkConfig.clientThreads shouldBe Some(2)
  }

  it should "produce child configurations" in {
    val rootConfig = ZookeeperClientConfig.fromConfig(appConfig.getConfig("zookeeper"))
    rootConfig.namespace shouldBe "akkeeper"

    rootConfig.child("") shouldBe rootConfig
    rootConfig.child("instance").namespace shouldBe "akkeeper/instance"
    rootConfig.copy(namespace = "akkeeper/")
      .child("instance").namespace shouldBe "akkeeper/instance"
  }
}
