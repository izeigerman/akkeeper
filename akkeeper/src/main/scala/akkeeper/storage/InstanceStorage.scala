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
package akkeeper.storage

import akkeeper.common._
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import akkeeper.storage.zookeeper.async.ZookeeperInstanceStorage
import scala.concurrent.Future

/** A persistent storage that stores information about existing instances. */
private[akkeeper] trait InstanceStorage[F[_]] extends Storage {

  /** Registers a new instance. Same instance can't be registered
    * more than once. The instance record must be removed automatically
    * once the client session is terminated. Session termination means
    * invocation of the [[Storage.stop()]] method.
    *
    * @param info the instance info. See [[InstanceInfo]].
    * @return a container object with the registered instance ID.
    */
  def registerInstance(info: InstanceInfo): F[InstanceId]

  /** Retrieves the information about the instance by its ID.
    *
    * @param instanceId the ID of the instance.
    * @return a container object with the instance's information.
    *         See [[InstanceInfo]].
    */
  def getInstance(instanceId: InstanceId): F[InstanceInfo]

  /** Retrieves all instances that belong to the specified
    * container.
    *
    * @param containerName the name of the container.
    * @return a container object with the list of instance IDs.
    */
  def getInstancesByContainer(containerName: String): F[Seq[InstanceId]]

  /** Retrieves all existing instances.
    *
    * @return a container object with the list of instance IDs.
    */
  def getInstances: F[Seq[InstanceId]]
}

private[akkeeper] object InstanceStorage {
  type Async = InstanceStorage[Future]
}

private[akkeeper] trait InstanceStorageFactory[F[_], T] extends (T => InstanceStorage[F])

private[akkeeper] object InstanceStorageFactory {
  type AsyncInstanceStorageFactory[T] = InstanceStorageFactory[Future, T]

  implicit object ZookeeperInstanceStorageFactory
    extends AsyncInstanceStorageFactory[ZookeeperClientConfig] {

    override def apply(config: ZookeeperClientConfig): InstanceStorage.Async = {
      new ZookeeperInstanceStorage(config.child("instances"))
    }
  }

  def createAsync[T: AsyncInstanceStorageFactory](config: T): InstanceStorage.Async = {
    implicitly[T](config)
  }
}
