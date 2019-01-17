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
package akkeeper.api

import java.util.UUID

import spray.json._

/** The unique ID of the API request.
  *
  * @param uuid the unique ID.
  */
case class RequestId(uuid: UUID) {
  override def toString: String = uuid.toString
}

object RequestId {
  def apply(): RequestId = RequestId(UUID.randomUUID())
}

trait RequestIdJsonProtocol extends DefaultJsonProtocol {
  implicit val requestIdFormat = new JsonFormat[RequestId] {

    override def write(obj: RequestId): JsValue = JsString(obj.toString)

    override def read(json: JsValue): RequestId = RequestId(UUID.fromString(json.convertTo[String]))
  }
}

object RequestIdJsonProtocol extends RequestIdJsonProtocol
