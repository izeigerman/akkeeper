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
package akkeeper.storage.zookeeper.async

import akkeeper.common._
import akkeeper.storage._
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import ContainerDefinitionJsonProtocol._
import org.apache.zookeeper.CreateMode
import scala.concurrent.Future

private[akkeeper] class ZookeeperContainerStorage(config: ZookeeperClientConfig)
  extends BaseZookeeperStorage with ContainerStorage.Async {

  protected override val zookeeperClient =
    new AsyncZookeeperClient(config, CreateMode.PERSISTENT)
  private implicit val executionContext = zookeeperClient.getExecutionContext

  override def createContainer(container: ContainerDefinition): Future[String] = {
    val path = container.name
    zookeeperClient
      .exists(path)
      .map(_ => throw RecordAlreadyExistsException(s"Container ${container.name} already exists"))
      .recoverWith {
        case _: RecordNotFoundException =>
          zookeeperClient.create(path, toBytes(container))
      }
  }

  override def updateContainer(container: ContainerDefinition): Future[String] = {
    val path = container.name
    zookeeperClient
      .exists(path)
      .flatMap(_ => {
        zookeeperClient.update(path, toBytes(container))
      })
  }

  override def getContainer(name: String): Future[ContainerDefinition] = {
    zookeeperClient
      .get(name)
      .map(fromBytes[ContainerDefinition])
  }

  override def getContainers: Future[Seq[String]] = {
    zookeeperClient
      .children("")
      .recover(notFoundToEmptySeq[String])
  }

  override def deleteContainer(name: String): Future[String] = {
    zookeeperClient.delete(name)
  }
}
