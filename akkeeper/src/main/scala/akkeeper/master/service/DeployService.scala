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

import akka.actor.{ActorRef, ActorRefFactory, Props, Stash}
import akka.pattern.pipe
import akkeeper.api._
import akkeeper.common._
import akkeeper.deploy._
import MonitoringService.InstancesUpdate
import akkeeper.master.service.DeployService.{InitFailed, InitSuccessful}

import scala.util.control.NonFatal

private[akkeeper] class DeployService(deployClient: DeployClient,
                                      containerService: ActorRef,
                                      monitoringService: ActorRef) extends RequestTrackingService with Stash {

  private implicit val dispatcher = context.dispatcher
  override protected val trackedMessages: Set[Class[_]] = Set(classOf[DeployContainer])

  private def deployInstances(request: DeployContainer,
                              container: ContainerDefinition): SubmittedInstances = {
    val ids = (0 until request.quantity).map(_ => InstanceId(container.name))
    val instanceInfos = ids.map(InstanceInfo.deploying)
    monitoringService ! InstancesUpdate(instanceInfos)

    val extendedContainer = container.copy(
      jvmArgs = request.jvmArgs.getOrElse(Seq.empty) ++ container.jvmArgs,
      jvmProperties = container.jvmProperties ++ request.properties.getOrElse(Map.empty))

    val futures = deployClient.deploy(extendedContainer, ids)
    val logger = log
    futures.foreach(f => {
      f.map {
        case DeploySuccessful(id) =>
          logger.debug(s"Instance $id deployed successfully")
          InstanceInfo.launching(id)
        case DeployFailed(id, e) =>
          logger.error(e, s"Deployment of instance $id failed")
          InstanceInfo.deployFailed(id)
      }.pipeTo(monitoringService)
    })
    SubmittedInstances(request.requestId, container.name, ids)
  }

  override def preStart(): Unit = {
    log.info("Starting deploy client initialization")

    deployClient.start()
      .map {_ => InitSuccessful()}
      .recover { case NonFatal(e) => InitFailed(e) }
      .pipeTo(self)

    super.preStart()
  }

  override def postStop(): Unit = {
    deployClient.stop()
    super.postStop()
  }

  def uninitializedReceive: Receive = {
    case InitSuccessful() =>
      log.info("Deploy client initialized")
      become(initializedReceive)
      unstashAll()
    case InitFailed(reason) =>
      log.error(reason, "Deploy client initialization failed")
      context.stop(self)
    case _: WithRequestId =>
      stash()
  }

  def apiCommandReceive: Receive = {
    case request: DeployContainer =>
      // Before launching a new instance we should first
      // retrieve an information about the container.
      setOriginalSenderContext(request.requestId, request)
      containerService ! GetContainer(request.name, requestId = request.requestId)
    case ContainerGetResult(id, container) =>
      // The information about the container was retrieved.
      // Now we can start the deployment process.
      val originalRequest = originalSenderContextAs[DeployContainer](id)
      val result = deployInstances(originalRequest, container)
      sendAndRemoveOriginalSender(result)
    case other: WithRequestId =>
      // Some unexpected response from the container service (likely error).
      // Just send it as is to the original sender.
      sendAndRemoveOriginalSender(other)
  }

  private def internalEventReceive: Receive = {
    case StopWithError(e) =>
      log.error("Stopping the Deploy service because of external error")
      deployClient.stopWithError(e)
      context.stop(self)
  }

  private def initializedReceive: Receive = {
    apiCommandReceive orElse internalEventReceive
  }

  override protected def serviceReceive: Receive = {
    uninitializedReceive orElse internalEventReceive
  }
}

object DeployService extends RemoteServiceFactory {
  private case class InitSuccessful()
  private case class InitFailed(reason: Throwable)

  override val actorName = "deployService"

  private[akkeeper] def createLocal(factory: ActorRefFactory,
                                    deployClient: DeployClient,
                                    containerService: ActorRef,
                                    monitoringService: ActorRef): ActorRef = {
    factory.actorOf(Props(classOf[DeployService], deployClient,
      containerService, monitoringService), actorName)
  }
}
