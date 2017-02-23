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
package akkeeper.api

import akkeeper.common._
import spray.json._

/** The base interface for all messages that include unique request ID. */
trait WithRequestId {
  def requestId: RequestId
}

/** A response message that represents an arbitrary general error.
  * It's usually used when some unexpected or unhandled error occurs.
  *
  * @param requestId the ID of the original request that caused an operation
  *                  that failed.
  * @param cause the reason of the failure.
  */
case class OperationFailed(requestId: RequestId, cause: Throwable) extends WithRequestId

/** JSON (de)serialization for the Common API requests and responses. */
trait CommonApiJsonProtocol extends DefaultJsonProtocol {
  import RequestIdJsonProtocol._

  implicit val operationFailedWriter = new JsonWriter[OperationFailed] {
    override def write(obj: OperationFailed): JsValue = {
      val requestIdField = obj.requestId.toJson
      val messageField = JsString(obj.cause.getMessage)
      val stackTraceField = JsArray(
        obj.cause.getStackTrace.map(s => JsString(s.toString)): _*
      )
      JsObject(
        "requestId" -> requestIdField,
        "message" -> messageField,
        "stackTrace" -> stackTraceField
      )
    }
  }
}
