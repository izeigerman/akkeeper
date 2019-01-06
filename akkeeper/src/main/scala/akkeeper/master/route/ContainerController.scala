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
package akkeeper.master.route

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import akkeeper.api._
import akkeeper.common.controller.BaseController
import akkeeper.common.{ContainerDefinition, ContainerDefinitionJsonProtocol}

import scala.concurrent.ExecutionContext

class ContainerController(override protected val service: ActorRef)(implicit timeout: Timeout)
  extends BaseController with ContainerApiJsonProtocol with ContainerDefinitionJsonProtocol {

  registerHandler[ContainerGetResult](StatusCodes.OK)
  registerHandler[ContainersList](StatusCodes.OK)
  registerHandler[ContainerUpdated](StatusCodes.OK)
  registerHandler[ContainerDeleted](StatusCodes.OK)
  registerHandler[ContainerCreated](StatusCodes.Created)
  registerHandler[ContainerNotFound](StatusCodes.NotFound)
  registerHandler[ContainerAlreadyExists](StatusCodes.Conflict)
  registerHandler[OperationFailed](StatusCodes.InternalServerError)

  override val route: Route =
    pathPrefix("containers") {
      path(Segment) { containerName =>
        get {
          handleRequest(GetContainer(containerName))
        } ~
        patch {
          entity(as[ContainerDefinition]) { definition =>
            handleRequest(UpdateContainer(definition))
          }
        } ~
        delete {
          handleRequest(DeleteContainer(containerName))
        }
      } ~
      (pathEnd | pathSingleSlash) {
        get {
          handleRequest(GetContainers())
        } ~
        post {
          entity(as[ContainerDefinition]) { definition =>
            handleRequest(CreateContainer(definition))
          }
        }
      }
    }
}

object ContainerController {
  def apply(service: ActorRef)(implicit dispatcher: ExecutionContext,
                               timeout: Timeout): ContainerController = {
    new ContainerController(service)
  }
}
