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

import java.net.ServerSocket

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import akkeeper.AwaitMixin
import scala.concurrent.Future
import scala.concurrent.duration._
import spray.json._

trait RestTestUtils extends AwaitMixin {

  this: TestKit =>

  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher
  implicit val timeout = Timeout(3 seconds)

  def allocatePort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def withHttpServer[T](route: Route)(f: Int => T): T = {
    val port = allocatePort
    val bindFuture = Http().bindAndHandle(route, "localhost", port)
    val result = bindFuture.map(b => {
      val result = f(port)
      b.unbind()
      result
    })
    await(result)
  }

  private def sendRequest(request: HttpRequest, port: Int): Future[(Int, String)] = {
    val connectionFlow = Http().outgoingConnection("localhost", port)
    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
      .flatMap(resp => resp.entity.toStrict(awaitTimeout).map(e => resp.status.intValue() -> e))
      .map {
        case (statusCode, entity) =>
          statusCode -> entity.getData().decodeString("UTF-8")
      }
  }

  private def deserialize[Resp: JsonReader](resp: Future[(Int, String)]): Future[(Int, Resp)] = {
    resp.map {
      case (statusCode, data) =>
        statusCode -> data.parseJson.convertTo[Resp]
    }
  }

  def postRaw[Req: JsonWriter](req: Req, uri: String, port: Int): Future[(Int, String)] = {
    val jsonBody = req.toJson.prettyPrint
    val request = HttpRequest(uri = uri)
      .withMethod(HttpMethods.POST)
      .withEntity(HttpEntity(ContentTypes.`application/json`, jsonBody))

    sendRequest(request, port)
  }

  def post[Req: JsonWriter, Resp: JsonReader](req: Req, uri: String,
                                              port: Int): Future[(Int, Resp)] = {
    deserialize(postRaw(req, uri, port))
  }

  def patchRaw[Req: JsonWriter](req: Req, uri: String, port: Int): Future[(Int, String)] = {
    val jsonBody = req.toJson.prettyPrint
    val request = HttpRequest(uri = uri)
      .withMethod(HttpMethods.PATCH)
      .withEntity(HttpEntity(ContentTypes.`application/json`, jsonBody))
    sendRequest(request, port)
  }

  def patch[Req: JsonWriter, Resp: JsonReader](req: Req, uri: String,
                                               port: Int): Future[(Int, Resp)] = {
    deserialize(patchRaw(req, uri, port))
  }

  def getRaw(uri: String, port: Int): Future[(Int, String)] = {
    val request = HttpRequest(uri = uri)
      .withMethod(HttpMethods.GET)
    sendRequest(request, port)
  }

  def get[Resp: JsonReader](uri: String, port: Int): Future[(Int, Resp)] = {
    deserialize(getRaw(uri, port))
  }

  def deleteRaw(uri: String, port: Int): Future[(Int, String)] = {
    val request = HttpRequest(uri = uri)
      .withMethod(HttpMethods.DELETE)
    sendRequest(request, port)
  }

  def delete[Resp: JsonReader](uri: String, port: Int): Future[(Int, Resp)] = {
    deserialize(deleteRaw(uri, port))
  }
}
