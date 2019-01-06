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
import akka.pattern.ask
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.server.{Directives, Route}
import akka.util.Timeout
import akkeeper.AkkeeperException
import akkeeper.common.api.CommonApiJsonProtocol
import spray.json.RootJsonWriter
import scala.collection.mutable
import scala.reflect.ClassTag

trait BaseController extends Directives with SprayJsonSupport with CommonApiJsonProtocol {

  def route: Route

  private val handlers: mutable.ArrayBuffer[PartialFunction[Any, Route]] =
    mutable.ArrayBuffer.empty

  final protected def registerHandler[T: RootJsonWriter: ClassTag](code: StatusCode): Unit = {
    handlers += {
      case v: T =>
        complete(code -> v)
    }
  }

  final protected def applyHandlers: PartialFunction[Any, Route] = {
    handlers.reduce(_ orElse _) orElse {
      case other =>
        val exception = new AkkeeperException(s"Unexpected response $other")
        failWith(exception)
    }
  }

  final protected def handleRequest(service: ActorRef, msg: Any)
                                   (implicit timeout: Timeout): Route = {
    val result = service ? msg
    onSuccess(result)(applyHandlers)
  }
}

