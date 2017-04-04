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
package akkeeper.master.route

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.server.PathMatcher._
import akka.http.scaladsl.server.{PathMatcher1, Route}
import akka.util.Timeout
import akkeeper.api._
import akkeeper.common.InstanceId
import scala.concurrent.ExecutionContext
import scala.util.Try
import MonitoringController._

class MonitoringController(service: ActorRef)(implicit dispatcher: ExecutionContext,
                                              timeout: Timeout)
  extends BaseController with MonitoringApiJsonProtocol {

  registerHandler[InstanceInfoResponse](StatusCodes.OK)
  registerHandler[InstancesList](StatusCodes.OK)
  registerHandler[InstanceNotFound](StatusCodes.NotFound)
  registerHandler[InstanceTerminated](StatusCodes.OK)

  override val route: Route =
    pathPrefix("instances") {
      path(InstanceIdMatcher) { instanceId =>
        get {
          handleRequest(service, GetInstance(instanceId))
        } ~
        delete {
          handleRequest(service, TerminateInstance(instanceId))
        }
      } ~
      (pathEnd | pathSingleSlash) {
        parameters('role.*, 'containerName.?) { (role, containerName) =>
          if (role.isEmpty && containerName.isEmpty) {
            handleRequest(service, GetInstances())
          } else {
            handleRequest(service, GetInstancesBy(role.toSet, containerName))
          }
        }
      }
    }
}

object MonitoringController {
  def apply(service: ActorRef)(implicit dispatcher: ExecutionContext,
                               timeout: Timeout): BaseController = {
    new MonitoringController(service)
  }

  private object InstanceIdMatcher extends PathMatcher1[InstanceId] {
    override def apply(v: Path): Matching[Tuple1[InstanceId]] = {
      if (!v.isEmpty) {
        val instanceId = Try(InstanceId.fromString(v.head.toString))
        instanceId.map(id => Matched(Path.Empty, Tuple1(id))).getOrElse(Unmatched)
      } else {
        Unmatched
      }
    }
  }
}
