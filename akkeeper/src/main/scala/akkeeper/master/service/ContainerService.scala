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
package akkeeper.master.service

import akka.actor._
import akkeeper.api._
import akkeeper.common.{ContainerDefinition, RequestId}
import akkeeper.config._
import scala.collection.mutable

private[akkeeper] class ContainerService extends Actor with ActorLogging {

  private val containers: mutable.Map[String, ContainerDefinition] = mutable.Map.empty

  override def preStart(): Unit = {
    loadContainersFromConfig()
    log.info("Container service successfully initialized")
    super.preStart()
  }

  private def loadContainersFromConfig(): Unit = {
    val config = context.system.settings.config
    val configContainers = config.akkeeper.containers
    configContainers.foreach(c => containers.put(c.name, c))
  }

  private def containerNotFound(requestId: RequestId, containerName: String): Any = {
    log.warning(s"Container with name $containerName was not found")
    ContainerNotFound(requestId, containerName)
  }

  private def createContainer(request: CreateContainer): Any = {
    val requestId = request.requestId
    val containerName = request.container.name
    if (containers.contains(containerName)) {
      log.warning(s"Container with name $containerName already exists")
      ContainerAlreadyExists(requestId, containerName)
    } else {
      containers.put(containerName, request.container)
      ContainerCreated(requestId, containerName)
    }
  }

  private def updateContainer(request: UpdateContainer): Any = {
    val requestId = request.requestId
    val containerName = request.container.name
    if (!containers.contains(containerName)) {
      containerNotFound(requestId, containerName)
    } else {
      containers.put(containerName, request.container)
      ContainerUpdated(requestId, containerName)
    }
  }

  private def deleteContainer(request: DeleteContainer): Any = {
    val requestId = request.requestId
    val containerName = request.name
    if (!containers.contains(containerName)) {
      containerNotFound(requestId, containerName)
    } else {
      containers.remove(containerName)
      ContainerDeleted(requestId, containerName)
    }
  }

  private def getContainer(request: GetContainer): Any = {
    val requestId = request.requestId
    val containerName = request.name
    if (!containers.contains(containerName)) {
      containerNotFound(requestId, containerName)
    } else {
      ContainerGetResult(requestId, containers(containerName))
    }
  }

  private def getContainers(request: GetContainers): Any = {
    ContainersList(request.requestId, containers.keys.toSeq)
  }

  override def receive: Receive = {
    case request: CreateContainer =>
      sender() ! createContainer(request)
    case request: UpdateContainer =>
      sender() ! updateContainer(request)
    case request: DeleteContainer =>
      sender() ! deleteContainer(request)
    case request: GetContainer =>
      sender() ! getContainer(request)
    case request: GetContainers =>
      sender() ! getContainers(request)
    case StopWithError(_) =>
      log.error("Stopping the Container service because of external error")
      context.stop(self)
  }
}

object ContainerService extends RemoteServiceFactory {
  override val actorName = "containerService"

  private[akkeeper] def createLocal(factory: ActorRefFactory): ActorRef = {
    factory.actorOf(Props(classOf[ContainerService]), actorName)
  }
}
