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

import java.util.concurrent.Executors
import akkeeper.storage._
import akkeeper.storage.zookeeper._
import AsyncZookeeperClient._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

private[zookeeper] class AsyncZookeeperClient(config: ZookeeperClientConfig,
                                              createMode: CreateMode)
  extends ZookeeperClient(config) {

  private val executor = Executors
    .newFixedThreadPool(config.clientThreads.getOrElse(DefaultClientThreads))

  final def create(path: String, data: Array[Byte]): Future[String] = {
    val (callback, future) = callbackWithFuture(_.getPath)
    client.create()
      .creatingParentsIfNeeded
      .withMode(createMode)
      .inBackground(callback, executor)
      .forPath(path, data)
    future
  }

  final def create(path: String): Future[String] = {
    create(path, Array.empty)
  }

  final def update(path: String, data: Array[Byte]): Future[String] = {
    val (callback, future) = callbackWithFuture(_.getPath)
    client.setData().inBackground(callback, executor).forPath(path, data)
    future
  }

  final def get(path: String): Future[Array[Byte]] = {
    val (callback, future) = callbackWithFuture(_.getData)
    client.getData.inBackground(callback, executor).forPath(path)
    future
  }

  final def delete(path: String): Future[String] = {
    val (callback, future) = callbackWithFuture(_.getPath)
    client.delete().inBackground(callback, executor).forPath(path)
    future
  }

  final def exists(path: String): Future[Stat] = {
    val (callback, future) = callbackWithFuture(_.getStat)
    client.checkExists().inBackground(callback, executor).forPath(path)
    future
  }

  final def children(path: String): Future[Seq[String]] = {
    val (callback, future) = callbackWithFuture(_.getChildren.asScala)
    client.getChildren.inBackground(callback, executor).forPath(path)
    future
  }

  final def getExecutionContext: ExecutionContext = {
    ExecutionContext.fromExecutor(executor)
  }

  override def stop(): Unit = {
    executor.shutdown()
    super.stop()
  }
}

object AsyncZookeeperClient {
  val DefaultClientThreads = 5

  private object ResultCodeErrorExtractor {
    def unapply(code: Int): Option[Throwable] = {
      Code.get(code) match {
        case Code.OK => None
        case Code.NONODE => Some(RecordNotFoundException("ZK node was not found"))
        case Code.NODEEXISTS => Some(RecordAlreadyExistsException("ZK node already exists"))
        case other =>
          Some(ZookeeperException(s"ZK operation failed (${other.toString})", other.intValue()))
      }
    }
  }

  private def asyncCallback[T](promise: Promise[T])(f: CuratorEvent => T): BackgroundCallback = {
    new BackgroundCallback {
      override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
        event.getResultCode match {
          case ResultCodeErrorExtractor(error) =>
            promise failure error
          case _ =>
            promise complete Try(f(event))
        }
      }
    }
  }

  private def callbackWithFuture[T](f: CuratorEvent => T): (BackgroundCallback, Future[T]) = {
    val promise = Promise[T]()
    val callback = asyncCallback(promise)(f)
    (callback, promise.future)
  }
}
