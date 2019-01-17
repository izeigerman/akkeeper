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

import scala.concurrent.ExecutionContext

class ContainerController(service: ActorRef)(implicit dispatcher: ExecutionContext,
                                             timeout: Timeout)
  extends BaseController with ContainerApiJsonProtocol with ContainerDefinitionJsonProtocol {

  registerHandler[ContainerGetResult](StatusCodes.OK)
  registerHandler[ContainersList](StatusCodes.OK)
  registerHandler[ContainerNotFound](StatusCodes.NotFound)
  registerHandler[OperationFailed](StatusCodes.InternalServerError)

  override val route: Route =
    pathPrefix("containers") {
      path(Segment) { containerName =>
        get {
          handleRequest(service, GetContainer(containerName))
        }
      } ~
      (pathEnd | pathSingleSlash) {
        get {
          handleRequest(service, GetContainers())
        }
      }
    }
}

object ContainerController {
  def apply(service: ActorRef)(implicit dispatcher: ExecutionContext,
                               timeout: Timeout): BaseController = {
    new ContainerController(service)
  }
}
