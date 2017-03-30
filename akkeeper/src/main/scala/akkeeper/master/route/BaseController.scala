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
import akka.pattern.ask
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.server.{Directives, Route}
import akka.util.Timeout
import akkeeper.api.CommonApiJsonProtocol
import spray.json.RootJsonWriter
import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}
import BaseController._

trait BaseController extends Directives with SprayJsonSupport with CommonApiJsonProtocol {

  def route: Route

  private val handlers: mutable.MutableList[Handler] = mutable.MutableList.empty

  final protected def registerHandler[T: RootJsonWriter: ClassTag](code: StatusCode): Unit = {
    handlers += new HandlerCons[T](code)
  }

  final protected def applyHandlers: PartialFunction[Any, Route] = {
    handlers
      .map(handler => {
        implicit val marshaller = handler.marshaller
        val classtag = handler.classtag
        val fun: PartialFunction[Any, Route] = {
          case classtag(v) =>
            complete(handler.statusCode -> v)
        }
        fun
      })
      .reduce(_ orElse _)
  }

  final protected def handleRequest(service: ActorRef, msg: Any)
                                   (implicit timeout: Timeout): Route = {
    val result = service ? msg
    onSuccess(result)(applyHandlers)
  }
}

object BaseController {
  private sealed trait Handler {
    type MessageType

    def marshaller: RootJsonWriter[MessageType]
    def classtag: ClassTag[MessageType]
    def statusCode: StatusCode
  }

  private sealed class HandlerCons[T: RootJsonWriter: ClassTag](override val statusCode: StatusCode)
    extends Handler {

    type MessageType = T

    override val classtag: ClassTag[MessageType] = classTag[T]
    override val marshaller: RootJsonWriter[MessageType] = implicitly[RootJsonWriter[T]]
  }
}
