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
package akkeeper.storage

import akkeeper.common.ContainerDefinition
import scala.concurrent.Future

/** A persistent storage that stores the information about containers. */
private[akkeeper] trait ContainerStorage[F[_]] extends Storage {

  /** Creates a new container.
    *
    * @param container the container's definition. See [[ContainerDefinition]].
    * @return a container object with the created container's name.
    */
  def createContainer(container: ContainerDefinition): F[String]

  /** Updates the existing container.
    *
    * @param container the container's definition. See [[ContainerDefinition]].
    * @return a container object with the updated container's name.
    */
  def updateContainer(container: ContainerDefinition): F[String]

  /** Deletes the existing container.
    *
    * @param name the name of the container that will be deleted.
    * @return a container object with the deleted container's name.
    */
  def deleteContainer(name: String): F[String]

  /** Retrieves the container's definition by its name.
    *
    * @param name the name of the container.
    * @return a container object with the requested container's definition.
    */
  def getContainer(name: String): F[ContainerDefinition]

  /** Retrieves the names of all existing containers.
    *
    * @return a container object with existing containers' names.
    */
  def getContainers: F[Seq[String]]
}

private[akkeeper] object ContainerStorage {
  type Async = ContainerStorage[Future]
}
