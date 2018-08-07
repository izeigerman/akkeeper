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
package akkeeper.storage.zookeeper

import com.typesafe.config.Config

private[akkeeper] case class ZookeeperClientConfig(servers: String,
                                                   connectionIntervalMs: Int,
                                                   maxRetries: Int,
                                                   clientThreads: Option[Int] = None,
                                                   namespace: String = "",
                                                   canBeReadOnly: Boolean = true) {
  def child(childNamespace: String): ZookeeperClientConfig = {
    if (childNamespace.nonEmpty) {
      val newNameSpace =
        if (namespace.endsWith("/")) {
          namespace + childNamespace
        } else {
          namespace + "/" + childNamespace
        }
      this.copy(namespace = newNameSpace)
    } else {
      this
    }
  }
}

private[akkeeper] object ZookeeperClientConfig {
  def fromConfig(zkConfig: Config): ZookeeperClientConfig = {
    val servers =
      if (zkConfig.hasPath("servers") && zkConfig.getString("servers").nonEmpty) {
        zkConfig.getString("servers")
      } else {
        sys.env.getOrElse("ZK_QUORUM", "localhost:2181")
      }
    val clientThreads =
      if (zkConfig.hasPath("client-threads")) {
        Some(zkConfig.getInt("client-threads"))
      } else {
        None
      }
    ZookeeperClientConfig(
      servers = servers,
      connectionIntervalMs = zkConfig.getInt("connection-interval-ms"),
      maxRetries = zkConfig.getInt("max-retries"),
      clientThreads = clientThreads,
      namespace = zkConfig.getString("namespace"),
      canBeReadOnly = zkConfig.getBoolean("can-be-readonly")
    )
  }
}
