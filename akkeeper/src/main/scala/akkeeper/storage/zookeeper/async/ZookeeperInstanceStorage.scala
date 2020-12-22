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

import akkeeper.api._
import akkeeper.api.InstanceInfoJsonProtocol._
import akkeeper.storage._
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import org.apache.zookeeper.CreateMode

import scala.concurrent.Future
import ZookeeperInstanceStorage._
import akka.actor.ActorSystem
import akkeeper.api.InstanceId

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

private[akkeeper] class ZookeeperInstanceStorage(config: ZookeeperClientConfig)(implicit system: ActorSystem)
  extends BaseZookeeperStorage with InstanceStorage {

  protected override val zookeeperClient =
    new AsyncZookeeperClient(config, CreateMode.EPHEMERAL)
  private implicit val executionContext = zookeeperClient.getExecutionContext

  override def registerInstance(status: InstanceInfo): Future[InstanceId] = {
    val instanceId = status.instanceId
    val path = instancePath(status)
    zookeeperClient
      .exists(path)
      .map(_ => throw RecordAlreadyExistsException(s"Instance $instanceId already exists"))
      .recoverWith {
        case _: RecordNotFoundException =>
          zookeeperClient.create(path, toBytes(status)).map(pathToInstanceId)
      }
  }

  override def getInstance(instanceId: InstanceId): Future[InstanceInfo] = {
    zookeeperClient
      .get(instanceId.containerName + "/" + instanceId.toString)
      .map(fromBytes[InstanceInfo])
  }

  //akka-http default timeout is 20 seconds, so ensure that the call does not exceed it
  //timeout outer future in 19 seconds
  override def getInstances: Future[Seq[InstanceId]] = {
    val t0 = System.nanoTime()
    val instancesFuture: Future[Seq[Future[Seq[String]]]] = for {
      //timeout
      containers <- zookeeperClient.children("")
    } yield for {
      container <- containers
    } yield zookeeperClient.children(container)
    val expectedExecution = instancesFuture
      .flatMap(f => Future.sequence(f).map(_.flatten))
      .map(_.map(pathToInstanceId))
      .recover(notFoundToEmptySeq[InstanceId])
    expectedExecution.onComplete { result:Try[Seq[InstanceId]] =>
      val t1 = System.nanoTime()
      val nanoDelta = t1 - t0
      val secondsDelta = nanoDelta.toDouble / 1000000000L
      val statusMsg = if(result.isSuccess) {"succeeded"} else {s"failed with ${result.failed.get}"}
      if(secondsDelta >= 20.0) {
        system.log.warning(s"getInstances TIMEOUTED, took ${secondsDelta} seconds and ${statusMsg}")
      } else {
        system.log.info(s"getInstances took ${secondsDelta} seconds and ${statusMsg}")
      }
    }(system.dispatcher)
    expectedExecution
  }

  override def getInstancesByContainer(containerName: String): Future[Seq[InstanceId]] = {
    zookeeperClient.children(containerName)
      .map(_.map(pathToInstanceId))
      .recover(notFoundToEmptySeq[InstanceId])
  }

}

private[akkeeper] object ZookeeperInstanceStorage {
  private def instancePath(status: InstanceInfo): String = {
    status.containerName + "/" + status.instanceId.toString
  }

  private def pathToInstanceId(path: String): InstanceId = {
    val split = path.split("/")
    val idString = if (split.size == 1) path else split.last
    InstanceId.fromString(idString)
  }
}
