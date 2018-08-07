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

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

private[zookeeper] abstract class ZookeeperClient(config: ZookeeperClientConfig) {
  protected val client: CuratorFramework = CuratorFrameworkFactory.builder()
    .namespace(config.namespace)
    .connectString(config.servers)
    .retryPolicy(new ExponentialBackoffRetry(config.connectionIntervalMs, config.maxRetries))
    .canBeReadOnly(config.canBeReadOnly)
    .build()

  def start(): Unit = {
    client.start()
  }

  def stop(): Unit = {
    client.close()
  }
}
